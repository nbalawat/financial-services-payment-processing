"""
Streaming processor for payment transactions.
Processes transactions from Kafka and stores them in HBase.
"""

import json
import logging
import happybase
from datetime import datetime
import functools
import time
from jsonschema import validate
import backoff
from kafka import KafkaConsumer
import signal
import sys
from typing import Dict, Any, List
from queue import Queue
from threading import Thread, Lock
import os
import glob
import socket

# Configuration
HBASE_HOST = 'localhost'
HBASE_PORT = 9091  # Updated to match docker-compose port mapping
HBASE_TABLE = 'payments'  # Updated to match the table name in create_table.hbase
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Payment schema for validation
PAYMENT_SCHEMA = {
    "type": "object",
    "required": ["payment_id", "payment_type", "amount", "originator", "created_at"],
    "properties": {
        "payment_id": {"type": "string"},
        "payment_type": {"type": "string"},
        "amount": {
            "type": "object",
            "required": ["value", "currency"],
            "properties": {
                "value": {"type": "number"},
                "currency": {"type": "string"}
            }
        },
        "originator": {
            "type": "object",
            "required": ["party_id", "name"],
            "properties": {
                "party_id": {"type": "string"},
                "name": {"type": "string"}
            }
        },
        "created_at": {"type": "string", "format": "date-time"}
    }
}

def timing_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logging.info(f"Step '{func.__name__}' completed in {elapsed_time:.2f} seconds")
        return result
    return wrapper

def check_port(host: str, port: int, timeout: float = 5.0) -> bool:
    """Check if a port is open on a host"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        logger.error(f"Error checking port {port} on {host}: {str(e)}")
        return False

def diagnose_hbase_connection(host: str = HBASE_HOST, port: int = HBASE_PORT):
    """Diagnose HBase connection issues"""
    logger.info(f"Diagnosing HBase connection to {host}:{port}")
    
    # Check if port is open
    if not check_port(host, port):
        logger.error(f"Port {port} is not open on {host}. HBase ThriftServer might not be running.")
        return False
    
    # Try to create a connection
    try:
        logger.info("Attempting to create HBase connection...")
        conn = happybase.Connection(host=host, port=port, timeout=10000)
        logger.info("Successfully created connection object")
        
        # Try to list tables
        try:
            logger.info("Attempting to list tables...")
            tables = conn.tables()
            logger.info(f"Available tables: {tables}")
            
            # Check if our table exists
            if bytes(HBASE_TABLE, 'utf-8') not in tables:
                logger.error(f"{HBASE_TABLE} table not found in HBase")
            else:
                logger.info(f"{HBASE_TABLE} table found")
                
            return True
        except Exception as e:
            logger.error(f"Error listing tables: {str(e)}")
            return False
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Error creating connection: {str(e)}")
        return False

class PaymentProcessor:
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS):
        self.bootstrap_servers = bootstrap_servers
        self.running = True
        self.consumer = None
        self.hbase_connection = None
        self.hbase_table = None
        self.metrics = {
            'processed_count': 0,
            'failed_count': 0,
            'total_latency': 0,
            'queued_count': 0,
            'recovered_count': 0
        }
        self.metrics_lock = Lock()
        
        # Create backup directory for failed writes
        self.backup_dir = "failed_payments"
        os.makedirs(self.backup_dir, exist_ok=True)
        
        # Queue for storing payments when HBase is unavailable
        self.payment_queue = Queue()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("\nShutting down processor...")
        self.running = False

    def setup_kafka(self):
        """Initialize Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                'payment-transactions',
                bootstrap_servers=self.bootstrap_servers,
                group_id='payment_processor_group',
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info("Successfully connected to Kafka")
            return True
        except Exception as e:
            logger.error(f"Error connecting to Kafka: {str(e)}")
            return False

    def setup_hbase(self):
        """Initialize HBase connection"""
        try:
            if self.hbase_connection:
                self.hbase_connection.close()
            
            # Run diagnostics first
            if not diagnose_hbase_connection():
                logger.error("HBase connection diagnostics failed")
                self.hbase_connection = None
                self.hbase_table = None
                return False
            
            logger.info("Creating HBase connection...")
            self.hbase_connection = happybase.Connection(
                host=HBASE_HOST,
                port=HBASE_PORT,
                timeout=10000  # 10 seconds timeout
            )
            
            # Verify the table exists
            tables = self.hbase_connection.tables()
            logger.info(f"Available HBase tables: {tables}")
            
            if bytes(HBASE_TABLE, 'utf-8') not in tables:
                logger.error(f"{HBASE_TABLE} table not found. Available tables: %s", tables)
                self.hbase_connection = None
                self.hbase_table = None
                return False
            
            self.hbase_table = self.hbase_connection.table(HBASE_TABLE)
            logger.info(f"Successfully connected to HBase and verified {HBASE_TABLE} table exists")
            
            # When HBase becomes available, load backed up payments
            self.load_backed_up_payments()
            return True
        except Exception as e:
            logger.error(f"Error connecting to HBase: {str(e)}")
            self.hbase_connection = None
            self.hbase_table = None
            return False

    def load_backed_up_payments(self):
        """Load backed up payments from disk when HBase becomes available"""
        try:
            backup_files = glob.glob(os.path.join(self.backup_dir, "*.json"))
            if backup_files:
                logger.info(f"Found {len(backup_files)} backed up payments to process")
                for file_path in backup_files:
                    try:
                        with open(file_path, 'r') as f:
                            payment = json.load(f)
                        
                        # Add to queue for processing
                        self.payment_queue.put(payment)
                        with self.metrics_lock:
                            self.metrics['queued_count'] += 1
                        
                        # Delete the backup file
                        os.remove(file_path)
                        logger.info(f"Loaded backed up payment from {file_path}")
                    except Exception as e:
                        logger.error(f"Error loading backed up payment {file_path}: {str(e)}")
        except Exception as e:
            logger.error(f"Error loading backed up payments: {str(e)}")

    def validate_payment(self, payment: Dict[str, Any]) -> bool:
        """Validate payment against schema"""
        try:
            validate(instance=payment, schema=PAYMENT_SCHEMA)
            return True
        except Exception as e:
            logger.error(f"Payment validation failed: {str(e)}")
            return False

    def backup_payment(self, payment: Dict[str, Any]):
        """Backup payment to disk if HBase is unavailable"""
        try:
            filename = f"{payment['payment_type']}-{payment['payment_id']}_{int(time.time())}.json"
            filepath = os.path.join(self.backup_dir, filename)
            with open(filepath, 'w') as f:
                json.dump(payment, f)
            logger.info(f"Backed up payment to {filepath}")
        except Exception as e:
            logger.error(f"Error backing up payment: {str(e)}")

    @timing_decorator
    def process_payment(self, payment: Dict[str, Any], is_recovery: bool = False):
        """Process a single payment"""
        try:
            # Add processing timestamp if not already present
            if 'processed_at' not in payment:
                payment['processed_at'] = datetime.now().isoformat()
            
            # Calculate processing latency
            generated_time = datetime.fromisoformat(payment['generated_at'])
            processed_time = datetime.fromisoformat(payment['processed_at'])
            latency_ms = (processed_time - generated_time).total_seconds() * 1000
            payment['processing_latency_ms'] = latency_ms
            
            # Create row key
            msg_timestamp = datetime.fromisoformat(payment['created_at']).strftime('%Y%m%d%H%M%S')
            row_key = f"{msg_timestamp}_{payment['payment_type']}_{payment['originator']['party_id']}"
            
            # Prepare HBase data
            data = {
                b'transaction:payment_type': str(payment['payment_type']),
                b'transaction:status': str(payment.get('status', 'RECEIVED')),
                b'transaction:amount': str(payment['amount']['value']),
                b'transaction:currency': str(payment['amount']['currency']),
                b'transaction:created_at': str(payment['created_at']),
                b'processing:generated_at': str(payment['generated_at']),
                b'processing:processed_at': str(payment['processed_at']),
                b'processing:latency_ms': str(payment['processing_latency_ms']),
                b'originator:party_id': str(payment['originator']['party_id']),
                b'originator:name': str(payment['originator']['name'])
            }
            
            # Add beneficiary info if present
            if 'beneficiary' in payment:
                data.update({
                    b'beneficiary:party_id': str(payment['beneficiary'].get('party_id', '')),
                    b'beneficiary:name': str(payment['beneficiary'].get('name', ''))
                })
            
            # Add type-specific fields
            if 'sec_code' in payment:  # ACH
                data[b'ach:sec_code'] = str(payment['sec_code'])
            elif 'wire_type' in payment:  # WIRE
                data[b'wire:type'] = str(payment['wire_type'])
            elif 'clearing_system' in payment:  # RTP
                data[b'rtp:clearing_system'] = str(payment['clearing_system'])
            
            # Try to write to HBase
            if self.hbase_table:
                try:
                    self.hbase_table.put(row_key.encode('utf-8'), data)
                    with self.metrics_lock:
                        if is_recovery:
                            self.metrics['recovered_count'] += 1
                        else:
                            self.metrics['processed_count'] += 1
                        self.metrics['total_latency'] += latency_ms
                    return True
                except Exception as e:
                    logger.error(f"Error writing to HBase: {str(e)}")
                    # Try to reconnect to HBase
                    if not self.setup_hbase():
                        # If reconnection fails, queue the payment
                        self.payment_queue.put(payment)
                        with self.metrics_lock:
                            self.metrics['queued_count'] += 1
                        self.backup_payment(payment)
                    return False
            else:
                # HBase not available, queue the payment
                self.payment_queue.put(payment)
                with self.metrics_lock:
                    self.metrics['queued_count'] += 1
                self.backup_payment(payment)
                return False
            
        except Exception as e:
            logger.error(f"Error processing payment: {str(e)}")
            with self.metrics_lock:
                self.metrics['failed_count'] += 1
            return False

    def process_queued_payments(self):
        """Process payments from the queue when HBase becomes available"""
        while self.running:
            if self.hbase_table and not self.payment_queue.empty():
                try:
                    payment = self.payment_queue.get_nowait()
                    if self.process_payment(payment, is_recovery=True):
                        with self.metrics_lock:
                            self.metrics['queued_count'] -= 1
                    self.payment_queue.task_done()
                except Exception:
                    pass
            time.sleep(1)

    def log_metrics(self):
        """Log processing metrics periodically"""
        with self.metrics_lock:
            processed = self.metrics['processed_count']
            failed = self.metrics['failed_count']
            queued = self.metrics['queued_count']
            recovered = self.metrics['recovered_count']
            total_latency = self.metrics['total_latency']
            
            if processed > 0:
                avg_latency = total_latency / processed
                logger.info(
                    f"Stats - Processed: {processed}, Failed: {failed}, "
                    f"Queued: {queued}, Recovered: {recovered}, "
                    f"Avg latency: {avg_latency:.2f}ms"
                )

    def run(self):
        """Run the payment processor"""
        try:
            # Setup Kafka connection
            if not self.setup_kafka():
                return
            
            # Try to setup HBase connection
            self.setup_hbase()
            
            # Start queue processing thread
            queue_thread = Thread(target=self.process_queued_payments)
            queue_thread.daemon = True
            queue_thread.start()
            
            logger.info("Starting payment processor...")
            last_metrics_time = time.time()
            last_hbase_check = time.time()
            
            while self.running:
                # Get messages from Kafka
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        if self.validate_payment(message.value):
                            self.process_payment(message.value)
                
                # Try to reconnect to HBase periodically if not connected
                current_time = time.time()
                if not self.hbase_table and current_time - last_hbase_check >= 30:
                    self.setup_hbase()
                    last_hbase_check = current_time
                
                # Log metrics every 10 seconds
                if current_time - last_metrics_time >= 10:
                    self.log_metrics()
                    last_metrics_time = current_time
                
        except Exception as e:
            logger.error(f"Error in processor: {str(e)}")
        finally:
            if self.consumer:
                self.consumer.close()
            if self.hbase_connection:
                self.hbase_connection.close()
            
            self.log_metrics()

if __name__ == "__main__":
    processor = PaymentProcessor()
    processor.run()
