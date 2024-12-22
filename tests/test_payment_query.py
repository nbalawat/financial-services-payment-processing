import unittest
from unittest.mock import Mock, patch
from datetime import datetime
from payment_query import PaymentQuery

class TestPaymentQuery(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_connection = Mock()
        self.mock_table = Mock()
        self.mock_connection.table.return_value = self.mock_table
        self.patcher = patch('payment_query.happybase.Connection', return_value=self.mock_connection)
        self.patcher.start()
        self.query = PaymentQuery()

    def tearDown(self):
        """Clean up after each test method."""
        self.patcher.stop()

    def test_date_validation(self):
        """Test date validation logic"""
        # Test invalid date format
        with self.assertRaises(ValueError):
            self.query.validate_date_range('2024/01/01', '2024-01-31')

        # Test end date before start date
        with self.assertRaises(ValueError):
            self.query.validate_date_range('2024-01-31', '2024-01-01')

        # Test valid date range
        self.assertTrue(self.query.validate_date_range('2024-01-01', '2024-01-31'))

    def test_row_key_creation(self):
        """Test row key prefix creation"""
        prefix = self.query._create_row_key_prefix('2024-01-01')
        self.assertEqual(prefix, '20240101')

        with self.assertRaises(ValueError):
            self.query._create_row_key_prefix('invalid-date')

    def test_query_by_time_range(self):
        """Test querying by time range"""
        # Mock scan results
        mock_data = [
            (b'20240101_ACH_123', {b'cf:amount': b'1000', b'cf:status': b'completed'}),
            (b'20240101_WIRE_456', {b'cf:amount': b'2000', b'cf:status': b'pending'})
        ]
        self.mock_table.scan.return_value = mock_data

        results = self.query.query_by_time_range('2024-01-01', '2024-01-31')
        self.assertEqual(len(results), 2)
        self.mock_table.scan.assert_called_once()

    def test_query_by_type_and_customer(self):
        """Test querying by payment type and customer"""
        # Mock scan results
        mock_data = [
            (b'20240101_ACH_123', {b'cf:amount': b'1000', b'cf:status': b'completed'})
        ]
        self.mock_table.scan.return_value = mock_data

        results = self.query.query_by_type_and_customer('ACH', 'CUST123')
        self.assertEqual(len(results), 1)
        self.mock_table.scan.assert_called_once()

    def test_error_handling(self):
        """Test error handling in queries"""
        # Test connection error
        self.mock_table.scan.side_effect = Exception("Connection error")
        with self.assertRaises(Exception):
            self.query.query_by_time_range('2024-01-01', '2024-01-31')

    def test_connection_cleanup(self):
        """Test connection cleanup"""
        self.query.close()
        self.mock_connection.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()
