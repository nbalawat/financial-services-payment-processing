import json
from kafka import KafkaProducer
import time
from datetime import datetime
import uuid
import random

def create_test_payment():
    payment_types = ['CREDIT', 'DEBIT', 'TRANSFER']
    currencies = ['USD', 'EUR', 'GBP']
    
    return {
        'payment_id': str(uuid.uuid4()),
        'payment_type': random.choice(payment_types),
        'amount': {
            'value': round(random.uniform(10, 1000), 2),
            'currency': random.choice(currencies)
        },
        'originator': {
            'party_id': str(random.randint(1000, 9999)),
            'name': f'Customer-{random.randint(1, 100)}'
        },
        'beneficiary': {
            'party_id': str(random.randint(1000, 9999)),
            'name': f'Merchant-{random.randint(1, 50)}'
        },
        'created_at': datetime.now().isoformat()
    }

def publish_test_messages():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    try:
        # Generate and send 20 test messages
        for i in range(20):
            payment = create_test_payment()
            producer.send('payment-transactions', payment)
            print(f"Sent payment {i+1}: {payment['payment_id']}")
            time.sleep(0.5)  # Wait half a second between messages
            
        producer.flush()  # Wait for messages to be sent
        print("All test messages sent successfully!")
        
    except Exception as e:
        print(f"Error sending messages: {str(e)}")
    finally:
        producer.close()

if __name__ == "__main__":
    publish_test_messages()
