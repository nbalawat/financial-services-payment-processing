import pytest
import time
from kafka import KafkaProducer, KafkaConsumer
import happybase
import psycopg2
from payment_producer import PaymentProducer
from payment_processor import PaymentProcessor
from payment_query import PaymentQuery

@pytest.mark.integration
class TestIntegration:
    @pytest.fixture(autouse=True)
    def setup(self, docker_compose_up):
        """Setup for each test"""
        # Wait for services to be fully ready
        time.sleep(10)
        
    def test_end_to_end_flow(self, kafka_config, hbase_config, sample_payment):
        """Test the entire flow from producer to consumer"""
        # 1. Produce a message to Kafka
        producer = PaymentProducer(kafka_config['bootstrap_servers'])
        producer.publish_payment(sample_payment)
        
        # 2. Verify message in Kafka
        consumer = KafkaConsumer(
            kafka_config['topic'],
            bootstrap_servers=kafka_config['bootstrap_servers'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )
        
        messages = list(consumer)
        assert len(messages) > 0
        
        # 3. Process message through Apache Beam
        processor = PaymentProcessor()
        processor.run()
        
        # 4. Verify data in HBase
        time.sleep(5)  # Wait for processing
        connection = happybase.Connection(host=hbase_config['host'], port=hbase_config['port'])
        table = connection.table(hbase_config['table'])
        
        # Create expected row key
        row_key = f"20240101120000_ACH_CUST123"
        row = table.row(row_key)
        assert row, "Payment not found in HBase"
        
        # 5. Query the data
        query = PaymentQuery()
        results = query.query_by_type_and_customer('ACH', 'CUST123')
        assert len(results) > 0
        assert results[0]['payment_id'] == sample_payment['payment_id']
        
    def test_error_recovery(self, kafka_config, sample_payment):
        """Test system recovery from errors"""
        # 1. Start with invalid data
        producer = PaymentProducer(kafka_config['bootstrap_servers'])
        invalid_payment = sample_payment.copy()
        del invalid_payment['payment_type']  # Make it invalid
        
        producer.publish_payment(invalid_payment)
        time.sleep(2)
        
        # 2. Send valid data
        producer.publish_payment(sample_payment)
        time.sleep(2)
        
        # 3. Verify only valid data was processed
        query = PaymentQuery()
        results = query.query_by_type_and_customer('ACH', 'CUST123')
        assert len(results) == 1
        
    def test_concurrent_processing(self, kafka_config):
        """Test system under concurrent load"""
        producer = PaymentProducer(kafka_config['bootstrap_servers'])
        
        # Generate and send multiple payments concurrently
        num_payments = 100
        for _ in range(num_payments):
            producer.generate_and_publish_continuous(
                transactions_per_batch=1,
                delay_seconds=0
            )
        
        time.sleep(10)  # Wait for processing
        
        # Verify all payments were processed
        query = PaymentQuery()
        results = query.query_by_time_range('2024-01-01', '2024-12-31')
        assert len(results) >= num_payments
        
    def test_data_consistency(self, kafka_config, sample_payment):
        """Test data consistency across the pipeline"""
        producer = PaymentProducer(kafka_config['bootstrap_servers'])
        
        # 1. Send same payment multiple times
        for _ in range(3):
            producer.publish_payment(sample_payment)
            time.sleep(1)
        
        # 2. Verify deduplication
        query = PaymentQuery()
        results = query.query_by_type_and_customer(
            sample_payment['payment_type'],
            sample_payment['originator_id']
        )
        
        # Should only have one record due to identical payment_id
        assert len(results) == 1
        
    def test_system_monitoring(self, kafka_config):
        """Test system monitoring and health checks"""
        # 1. Check Kafka health
        producer = KafkaProducer(bootstrap_servers=kafka_config['bootstrap_servers'])
        assert producer.bootstrap_connected()
        
        # 2. Check HBase health
        connection = happybase.Connection(host='localhost', port=16000)
        tables = connection.tables()
        assert b'payments' in tables
        
        # 3. Check PostgreSQL health
        conn = psycopg2.connect(
            dbname='payment_benchmark',
            user='benchmark_user',
            password='benchmark_pass',
            host='localhost'
        )
        cursor = conn.cursor()
        cursor.execute('SELECT 1')
        assert cursor.fetchone()[0] == 1
