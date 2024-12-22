import unittest
from unittest.mock import Mock, patch
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from payment_processor import PaymentNormalizer, PaymentProcessor

class TestPaymentProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.processor = PaymentProcessor()

    def test_payment_normalizer(self):
        """Test the PaymentNormalizer DoFn"""
        with TestPipeline() as p:
            input_data = [
                '{"payment_id": "123", "payment_type": "ACH", "amount": 1000, '
                '"currency": "USD", "originator_id": "CUST123", '
                '"created_at": "2024-01-01T12:00:00"}'
            ]

            output = (
                p
                | beam.Create(input_data)
                | beam.ParDo(PaymentNormalizer())
            )

            expected_output = [
                ('20240101120000_ACH_CUST123', {
                    'payment_id': '123',
                    'payment_type': 'ACH',
                    'amount': '1000',
                    'currency': 'USD',
                    'originator_id': 'CUST123',
                    'created_at': '2024-01-01T12:00:00'
                })
            ]

            assert_that(output, equal_to(expected_output))

    def test_invalid_json_handling(self):
        """Test handling of invalid JSON input"""
        with TestPipeline() as p:
            input_data = ['invalid json data']
            output = (
                p
                | beam.Create(input_data)
                | beam.ParDo(PaymentNormalizer())
            )
            assert_that(output, equal_to([]))

    def test_missing_fields_handling(self):
        """Test handling of missing required fields"""
        with TestPipeline() as p:
            input_data = ['{"payment_id": "123"}']  # Missing required fields
            output = (
                p
                | beam.Create(input_data)
                | beam.ParDo(PaymentNormalizer())
            )
            assert_that(output, equal_to([]))

    @patch('payment_processor.WriteToHBase')
    def test_hbase_sink_configuration(self, mock_write_to_hbase):
        """Test HBase sink configuration"""
        sink = self.processor.create_hbase_sink()
        mock_write_to_hbase.assert_called_once()
        args = mock_write_to_hbase.call_args[0]
        self.assertEqual(args[0], 'payment_transactions')
        self.assertEqual(args[1], 'cf')

    def test_pipeline_options(self):
        """Test pipeline options configuration"""
        options = self.processor.setup_pipeline().options
        self.assertEqual(options.runner, 'DirectRunner')
        self.assertTrue(options.streaming)

    @patch('payment_processor.ReadFromKafka')
    def test_kafka_source_configuration(self, mock_read_from_kafka):
        """Test Kafka source configuration"""
        source = self.processor.create_kafka_source()
        mock_read_from_kafka.assert_called_once()
        config = mock_read_from_kafka.call_args[1]['consumer_config']
        self.assertEqual(config['bootstrap.servers'], 'localhost:9092')
        self.assertEqual(config['group.id'], 'payment_processor_group')

if __name__ == '__main__':
    unittest.main()
