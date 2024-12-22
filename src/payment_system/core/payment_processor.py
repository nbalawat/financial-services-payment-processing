import apache_beam as beam
from apache_beam import window
from apache_beam.io import iobase
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker
from apache_beam.transforms import trigger
from kafka import KafkaConsumer
from queue import Queue
import json
import logging
import time
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.metrics import Metrics, MetricsFilter
from datetime import datetime
import functools
from jsonschema import validate
import backoff
import happybase
import threading

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

class KafkaSource(iobase.BoundedSource):
    def __init__(self, bootstrap_servers, topic, group_id):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.message_queue = Queue(maxsize=1000)
        self.should_stop = False
        self.weight = 1.0  # Default weight for splitting

    def estimate_size(self):
        return None  # Size cannot be estimated for Kafka source

    def get_range_tracker(self, start_position, stop_position):
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = float('inf')
        return OffsetRestrictionTracker(OffsetRange(start_position, stop_position))

    def read(self, range_tracker):
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: x  # Keep as bytes, deserialize later
        )

        try:
            position = range_tracker.start_position()
            for message in consumer:
                if not range_tracker.try_claim(position):
                    return
                position += 1
                yield message.value
        finally:
            consumer.close()

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = float('inf')
        # For Kafka, we don't actually split the source
        bundle = iobase.SourceBundle(
            weight=self.weight,
            source=self,
            start_position=start_position,
            stop_position=stop_position)
        return [bundle]

class HBaseWriter(beam.DoFn):
    def __init__(self):
        super().__init__()
        self.connection = None
        self.table = None

    def setup(self):
        try:
            self.connection = happybase.Connection('localhost', port=9091, protocol='binary', transport='buffered')
            self.table = self.connection.table('payments')
            logging.info("Successfully connected to HBase")
        except Exception as e:
            logging.error(f"Failed to connect to HBase: {str(e)}")
            raise

    def teardown(self):
        if self.connection:
            self.connection.close()

    @backoff.on_exception(backoff.expo, Exception, max_tries=5)
    def process(self, element):
        try:
            if not self.connection:
                self.setup()

            # Element is already a tuple of (row_key, data)
            row_key, data = element
            logging.info("Writing to HBase - Key: %s, Data: %s", row_key, data)
            
            # Prepare mutations for HBase
            mutations = {
                f'cf:{k}': str(v) for k, v in data.items()
            }
            
            # Write to HBase
            self.table.put(row_key.encode('utf-8'), mutations)
            logging.info("Successfully wrote payment %s to HBase", data.get('payment_id'))
            
        except Exception as e:
            logging.error("Error writing to HBase: %s", str(e), exc_info=True)
            raise

class PaymentNormalizer(beam.DoFn):
    def process(self, element):
        try:
            logging.info("Normalizing payment: %s", element)
            # Parse the Kafka message
            payment = json.loads(element.decode('utf-8'))
            
            # Create row key: timestamp_paymenttype_customerid
            msg_timestamp = datetime.fromisoformat(payment['created_at']).strftime('%Y%m%d%H%M%S')
            row_key = f"{msg_timestamp}_{payment['payment_type']}_{payment['originator']['party_id']}"
            
            # Flatten the payment data
            data = {
                'payment_id': payment['payment_id'],
                'payment_type': payment['payment_type'],
                'amount_value': str(payment['amount']['value']),
                'amount_currency': payment['amount']['currency'],
                'originator_id': payment['originator']['party_id'],
                'originator_name': payment['originator']['name'],
                'created_at': payment['created_at']
            }
            
            # Add beneficiary info if present
            if 'beneficiary' in payment:
                data.update({
                    'beneficiary_id': payment['beneficiary'].get('party_id', ''),
                    'beneficiary_name': payment['beneficiary'].get('name', '')
                })
            
            logging.info("Normalized payment: key=%s, data=%s", row_key, data)
            yield (row_key, data)
            
        except Exception as e:
            logging.error("Error normalizing payment: %s", str(e), exc_info=True)
            raise

class PaymentProcessor:
    def __init__(self):
        self.start_time = None

    def setup_pipeline(self):
        # Initialize pipeline options for streaming
        pipeline_options = PipelineOptions()
        pipeline_options.view_as(StandardOptions).streaming = True
        return beam.Pipeline(options=pipeline_options)

    def run(self):
        try:
            # Set up the pipeline options
            options = PipelineOptions()
            options.view_as(StandardOptions).streaming = True
            
            # Create the pipeline
            pipeline = beam.Pipeline(options=options)
            
            # Create Kafka source
            kafka_source = KafkaSource(
                bootstrap_servers='localhost:9092',
                topic='payment-transactions',
                group_id='payment_processor_group'
            )
            
            # Build and run the pipeline
            logging.info("Starting pipeline...")
            (pipeline
                | "Read from Kafka" >> beam.io.Read(kafka_source)
                | "Window" >> beam.WindowInto(
                    window.FixedWindows(60),  # 60-second windows
                    trigger=trigger.Repeatedly(trigger.AfterCount(100)),  # Trigger after every 100 elements
                    accumulation_mode=trigger.AccumulationMode.DISCARDING
                )
                | "Normalize Payments" >> beam.ParDo(PaymentNormalizer())
                | "Write to HBase" >> beam.ParDo(HBaseWriter())
            )
            
            logging.info("Running pipeline...")
            result = pipeline.run()
            result.wait_until_finish()
            logging.info("Pipeline completed.")
            
        except Exception as e:
            logging.error("Unexpected error in pipeline execution: %s", str(e), exc_info=True)
            raise

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    processor = PaymentProcessor()
    processor.run()
