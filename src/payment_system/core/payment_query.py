import happybase
from datetime import datetime
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PaymentQuery:
    def __init__(self, host='localhost', port=16000):
        try:
            self.connection = happybase.Connection(host=host, port=port)
            self.table = self.connection.table('payments')
            # Verify table exists
            if b'payments' not in self.connection.tables():
                raise ValueError("'payments' table does not exist in HBase")
        except ConnectionError as e:
            logger.error(f"Failed to connect to HBase at {host}:{port}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error initializing PaymentQuery: {str(e)}")
            raise

    def _create_row_key_prefix(self, date_str):
        """Create a row key prefix from a date string"""
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y%m%d')
        except ValueError as e:
            logger.error(f"Invalid date format. Expected 'YYYY-MM-DD', got '{date_str}': {str(e)}")
            raise

    def validate_date_range(self, start_date, end_date):
        """Validate date range parameters"""
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            if end < start:
                raise ValueError("End date must be after start date")
            return True
        except ValueError as e:
            logger.error(f"Date validation error: {str(e)}")
            raise

    def _scan_table(self, start_key, end_key, batch_size=1000):
        """Scan HBase table with error handling"""
        try:
            results = []
            for key, data in self.table.scan(row_start=start_key, row_stop=end_key, batch_size=batch_size):
                try:
                    payment_data = {
                        col.decode('utf-8').split(':')[1]: val.decode('utf-8')
                        for col, val in data.items()
                    }
                    payment_data['row_key'] = key.decode('utf-8')
                    results.append(payment_data)
                except UnicodeDecodeError as e:
                    logger.error(f"Failed to decode payment data for key {key}: {str(e)}")
                    continue
            return results
        except Exception as e:
            logger.error(f"Error scanning HBase table: {str(e)}")
            raise

    def query_by_time_range(self, start_date, end_date):
        """Query payments by time range"""
        try:
            self.validate_date_range(start_date, end_date)
            start_key = self._create_row_key_prefix(start_date)
            end_key = self._create_row_key_prefix(end_date) + 'zzz'
            
            logger.info(f"Querying payments between {start_date} and {end_date}")
            results = self._scan_table(start_key, end_key)
            
            if not results:
                logger.warning(f"No payments found between {start_date} and {end_date}")
            else:
                logger.info(f"Found {len(results)} payments")
            
            return results
        except ValueError as e:
            logger.error(f"Invalid date range: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error querying by time range: {str(e)}")
            raise

    def query_by_type_and_customer(self, payment_type, customer_id):
        """Query payments by type and customer"""
        try:
            if not payment_type or not customer_id:
                raise ValueError("Both payment_type and customer_id must be provided")
                
            logger.info(f"Querying payments for type {payment_type} and customer {customer_id}")
            
            filter_string = (
                f"SingleColumnValueFilter('cf', 'payment_type', =, 'binary:{payment_type}') AND "
                f"SingleColumnValueFilter('cf', 'originator_id', =, 'binary:{customer_id}')"
            )
            
            results = []
            for key, data in self.table.scan(filter=filter_string):
                try:
                    payment_data = {
                        col.decode('utf-8').split(':')[1]: val.decode('utf-8')
                        for col, val in data.items()
                    }
                    payment_data['row_key'] = key.decode('utf-8')
                    results.append(payment_data)
                except UnicodeDecodeError as e:
                    logger.error(f"Failed to decode payment data for key {key}: {str(e)}")
                    continue
            
            if not results:
                logger.warning(f"No payments found for type {payment_type} and customer {customer_id}")
            else:
                logger.info(f"Found {len(results)} payments")
            
            return results
        except ValueError as e:
            logger.error(f"Invalid query parameters: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error querying by type and customer: {str(e)}")
            raise

    def query_by_date_type_customer(self, start_date, end_date, payment_type, customer_id):
        """Query payments by date range, type, and customer"""
        try:
            self.validate_date_range(start_date, end_date)
            start_key = self._create_row_key_prefix(start_date)
            end_key = self._create_row_key_prefix(end_date) + 'zzz'
            
            logger.info(
                f"Querying payments between {start_date} and {end_date} "
                f"for type {payment_type} and customer {customer_id}"
            )
            
            filter_string = (
                f"SingleColumnValueFilter('cf', 'payment_type', =, 'binary:{payment_type}') AND "
                f"SingleColumnValueFilter('cf', 'originator_id', =, 'binary:{customer_id}')"
            )
            
            results = []
            for key, data in self.table.scan(row_start=start_key, row_stop=end_key, filter=filter_string):
                try:
                    payment_data = {
                        col.decode('utf-8').split(':')[1]: val.decode('utf-8')
                        for col, val in data.items()
                    }
                    payment_data['row_key'] = key.decode('utf-8')
                    results.append(payment_data)
                except UnicodeDecodeError as e:
                    logger.error(f"Failed to decode payment data for key {key}: {str(e)}")
                    continue
            
            if not results:
                logger.warning(f"No payments found between {start_date} and {end_date} for type {payment_type} and customer {customer_id}")
            else:
                logger.info(f"Found {len(results)} payments")
            
            return results
        except ValueError as e:
            logger.error(f"Invalid query parameters: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error querying by date, type, and customer: {str(e)}")
            raise

    def close(self):
        """Close the HBase connection"""
        try:
            self.connection.close()
            logger.info("HBase connection closed successfully")
        except Exception as e:
            logger.error(f"Error closing HBase connection: {str(e)}")
            raise

def main():
    try:
        query = PaymentQuery()
        
        # Example queries
        try:
            # Query by date range
            start_date = '2024-01-01'
            end_date = '2024-01-31'
            results = query.query_by_time_range(start_date, end_date)
            print(f"\nFound {len(results)} payments between {start_date} and {end_date}")
            
            # Query by type and customer
            payment_type = 'ACH'
            customer_id = '12345'
            results = query.query_by_type_and_customer(payment_type, customer_id)
            print(f"\nFound {len(results)} {payment_type} payments for customer {customer_id}")
            
            # Query by date, type, and customer
            start_date = '2024-01-01'
            end_date = '2024-01-31'
            payment_type = 'ACH'
            customer_id = '12345'
            results = query.query_by_date_type_customer(start_date, end_date, payment_type, customer_id)
            print(f"\nFound {len(results)} {payment_type} payments for customer {customer_id} between {start_date} and {end_date}")
            
        except Exception as e:
            logger.error(f"Error executing queries: {str(e)}")
        finally:
            query.close()
            
    except Exception as e:
        logger.error("Failed to initialize PaymentQuery")
        logger.exception("Full traceback:")
        sys.exit(1)

if __name__ == "__main__":
    main()
