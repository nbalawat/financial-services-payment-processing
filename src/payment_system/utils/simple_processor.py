import json
import logging
from kafka import KafkaConsumer
import happybase
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def process_messages():
    # Connect to HBase
    connection = happybase.Connection('localhost', port=9091, protocol='binary', transport='buffered')
    table = connection.table('payments')
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        'payment-transactions',
        bootstrap_servers='localhost:9092',
        group_id='simple_processor_group',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    try:
        logging.info("Starting to consume messages...")
        for message in consumer:
            try:
                payment = message.value
                
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
                
                # Prepare mutations for HBase
                mutations = {
                    f'cf:{k}': str(v) for k, v in data.items()
                }
                
                # Write to HBase
                table.put(row_key.encode('utf-8'), mutations)
                logging.info(f"Successfully processed payment {payment['payment_id']}")
                
            except Exception as e:
                logging.error(f"Error processing message: {str(e)}")
                continue
                
    except KeyboardInterrupt:
        logging.info("Shutting down...")
    finally:
        consumer.close()
        connection.close()

if __name__ == "__main__":
    process_messages()
