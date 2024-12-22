from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError, KafkaError
import json
from datetime import datetime
import time
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

class PaymentProducer:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                key_serializer=lambda x: x.encode('utf-8'),
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
        except Exception as e:
            logger.error(f"Failed to initialize PaymentProducer: {str(e)}")
            raise

    def publish_payment(self, payment_dict, topic='payment-transactions'):
        try:
            # Validate payment data with nested structure
            if not payment_dict.get('payment_id'):
                raise ValueError("Missing payment_id")
            if not payment_dict.get('payment_type'):
                raise ValueError("Missing payment_type")
            if not payment_dict.get('amount', {}).get('value'):
                raise ValueError("Missing amount.value")
            if not payment_dict.get('amount', {}).get('currency'):
                raise ValueError("Missing amount.currency")
            if not payment_dict.get('originator', {}).get('party_id'):
                raise ValueError("Missing originator.party_id")

            # Use payment_type as key for grouping similar payment types
            key = payment_dict['payment_type']
            
            future = self.producer.send(topic, key=key, value=payment_dict)
            self.producer.flush()
            record_metadata = future.get(timeout=10)
            logger.info(
                f"Published payment {payment_dict['payment_id']} of type {key} "
                f"to topic {topic} partition {record_metadata.partition} offset {record_metadata.offset}"
            )
        except ValueError as e:
            logger.error(f"Payment validation error: {str(e)}")
        except KafkaTimeoutError as e:
            logger.error(f"Kafka timeout error while publishing payment: {str(e)}")
        except KafkaError as e:
            logger.error(f"Kafka error while publishing payment: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error while publishing payment: {str(e)}")
            logger.exception("Full traceback:")

    def publish_from_file(self, file_path, delay_seconds=0.1):
        """Publish payments from a JSON file"""
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Transaction file not found: {file_path}")

            with open(file_path, 'r') as f:
                data = json.load(f)
                
            if 'transactions' not in data:
                raise ValueError("Invalid JSON format: 'transactions' key not found")
                
            transactions = data['transactions']
            total_transactions = len(transactions)
            logger.info(f"Starting to publish {total_transactions} transactions")
            
            for i, transaction in enumerate(transactions, 1):
                self.publish_payment(transaction)
                if delay_seconds > 0:
                    time.sleep(delay_seconds)
                if i % 10 == 0:  # Progress update every 10 transactions
                    logger.info(f"Published {i}/{total_transactions} transactions")
            
            logger.info(f"Finished publishing all {total_transactions} transactions")
            
        except Exception as e:
            logger.error(f"Error publishing from file: {str(e)}")
            raise

if __name__ == "__main__":
    producer = PaymentProducer()
    transactions_file = "../../data/generated_transactions/transactions.json"
    producer.publish_from_file(transactions_file)
