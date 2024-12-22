"""
Continuous streaming producer for payment transactions.
Generates and sends payment transactions to Kafka in real-time.
"""

import json
import time
import random
import logging
import signal
import sys
import argparse
from datetime import datetime
from kafka import KafkaProducer
from payment_system.core.transaction_generator import TransactionGenerator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class StreamingProducer:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.running = True
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8')
        )
        self.generator = TransactionGenerator(num_customers=1000)
        self.payment_types = ['ACH', 'WIRE', 'CHECK', 'RTP', 'SEPA', 'CARD', 'DIRECT_DEBIT']
        self.transaction_count = 0
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("\nShutting down producer...")
        self.running = False

    def generate_transaction(self):
        """Generate a random transaction"""
        payment_type = random.choice(self.payment_types)
        
        if payment_type == 'ACH':
            payment = self.generator.generate_ach_payment()
        elif payment_type == 'WIRE':
            payment = self.generator.generate_wire_payment()
        elif payment_type == 'CHECK':
            payment = self.generator.generate_check_payment()
        elif payment_type == 'RTP':
            payment = self.generator.generate_rtp_payment()
        elif payment_type == 'SEPA':
            payment = self.generator.generate_sepa_payment()
        elif payment_type == 'CARD':
            payment = self.generator.generate_card_payment()
        else:  # DIRECT_DEBIT
            payment = self.generator.generate_direct_debit_payment()
            
        return payment

    def run(self, transactions_per_second=5):
        """
        Run the streaming producer
        Args:
            transactions_per_second: Number of transactions to generate per second
        """
        logger.info(f"Starting streaming producer with {transactions_per_second} transactions per second")
        delay = 1.0 / transactions_per_second

        try:
            while self.running:
                transaction = self.generate_transaction()
                
                # Convert payment to dict and add timestamp
                transaction_dict = transaction.to_dict()
                transaction_dict['generated_at'] = datetime.now().isoformat()
                
                # Send to Kafka
                future = self.producer.send(
                    'payment-transactions',
                    key=transaction_dict['payment_id'],
                    value=transaction_dict
                )
                
                # Wait for the message to be delivered
                future.get(timeout=10)
                
                self.transaction_count += 1
                if self.transaction_count % transactions_per_second == 0:
                    logger.info(f"Generated {self.transaction_count} transactions")
                
                time.sleep(delay)

        except Exception as e:
            logger.error(f"Error in producer: {str(e)}")
        finally:
            self.producer.close()
            logger.info(f"Producer stopped. Total transactions generated: {self.transaction_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start the streaming payment transaction producer')
    parser.add_argument('--tps', type=int, default=5, help='Transactions per second (default: 5)')
    parser.add_argument('--customers', type=int, default=1000, help='Number of customers to simulate (default: 1000)')
    args = parser.parse_args()

    producer = StreamingProducer()
    try:
        producer.run(transactions_per_second=args.tps)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    finally:
        sys.exit(0)
