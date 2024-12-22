import unittest
from unittest.mock import Mock, patch
import json
from payment_producer import PaymentProducer

class TestPaymentProducer(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_kafka_producer = Mock()
        self.patcher = patch('payment_producer.KafkaProducer', return_value=self.mock_kafka_producer)
        self.patcher.start()
        self.producer = PaymentProducer(['localhost:9092'])

    def tearDown(self):
        """Clean up after each test method."""
        self.patcher.stop()

    def test_publish_payment_validation(self):
        """Test payment validation before publishing"""
        invalid_payment = {'payment_id': '123'}  # Missing required fields
        self.producer.publish_payment(invalid_payment)
        self.mock_kafka_producer.send.assert_not_called()

        valid_payment = {
            'payment_id': '123',
            'payment_type': 'ACH',
            'amount': 1000,
            'currency': 'USD',
            'originator_id': 'CUST123'
        }
        self.producer.publish_payment(valid_payment)
        self.mock_kafka_producer.send.assert_called_once()

    def test_kafka_producer_configuration(self):
        """Test Kafka producer configuration"""
        with patch('payment_producer.KafkaProducer') as mock_kafka:
            PaymentProducer(['localhost:9092'])
            mock_kafka.assert_called_once()
            config = mock_kafka.call_args[1]
            self.assertEqual(config['bootstrap_servers'], ['localhost:9092'])
            self.assertIn('value_serializer', config)

    @patch('payment_producer.time.sleep', return_value=None)
    def test_continuous_generation(self, mock_sleep):
        """Test continuous transaction generation"""
        self.producer.generate_and_publish_continuous(
            transactions_per_batch=2,
            delay_seconds=0
        )
        self.assertTrue(self.mock_kafka_producer.send.called)
        self.assertGreater(self.mock_kafka_producer.send.call_count, 0)

    def test_producer_error_handling(self):
        """Test error handling in producer"""
        self.mock_kafka_producer.send.side_effect = Exception("Kafka connection error")
        valid_payment = {
            'payment_id': '123',
            'payment_type': 'ACH',
            'amount': 1000,
            'currency': 'USD',
            'originator_id': 'CUST123'
        }
        # Should not raise exception but log error
        self.producer.publish_payment(valid_payment)

    def test_producer_shutdown(self):
        """Test graceful producer shutdown"""
        self.producer.generate_and_publish_continuous()
        self.mock_kafka_producer.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()
