import unittest
from datetime import datetime
import pandas as pd
from generate_transactions import TransactionGenerator

class TestTransactionGenerator(unittest.TestCase):
    def setUp(self):
        """Initialize the transaction generator before each test"""
        self.generator = TransactionGenerator(num_customers=10)  # Small number for testing

    def test_customer_generation(self):
        """Test if customer data is generated correctly"""
        self.assertGreater(len(self.generator.customers), 0)
        sample_customer = self.generator.customers[0]
        required_fields = ['customer_id', 'name', 'country', 'currency']
        for field in required_fields:
            self.assertIn(field, sample_customer)

    def test_payment_type_generation(self):
        """Test if different payment types are generated correctly"""
        payment_types = ['ACH', 'WIRE', 'CHECK', 'INTERNAL_TRANSFER']
        transactions = self.generator.generate_all_payment_types(10)
        self.assertIsInstance(transactions, pd.DataFrame)
        self.assertTrue(all(pt in transactions['payment_type'].unique() for pt in payment_types))

    def test_amount_ranges(self):
        """Test if generated amounts are within expected ranges"""
        transactions = self.generator.generate_all_payment_types(100)
        self.assertTrue(all(transactions['amount'] > 0))
        self.assertTrue(all(transactions['amount'] < 1000000))  # Adjust based on your max amount

    def test_date_generation(self):
        """Test if generated dates are valid and within range"""
        transactions = self.generator.generate_all_payment_types(10)
        for date_str in transactions['created_at']:
            date = datetime.fromisoformat(date_str)
            self.assertIsInstance(date, datetime)
            self.assertGreaterEqual(date.year, 2024)

    def test_unique_payment_ids(self):
        """Test if generated payment IDs are unique"""
        transactions = self.generator.generate_all_payment_types(100)
        self.assertEqual(len(transactions['payment_id'].unique()), len(transactions))

if __name__ == '__main__':
    unittest.main()
