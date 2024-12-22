import happybase
import logging
import time

logging.basicConfig(level=logging.INFO)

def create_table():
    connection = None
    max_retries = 5
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            # Connect to HBase using localhost since we're port forwarding
            connection = happybase.Connection('localhost', port=9091, protocol='binary', transport='buffered')
            
            # Get list of existing tables
            existing_tables = connection.tables()
            table_name = b'payments'
            
            if table_name in existing_tables:
                logging.info("Table 'payments' exists")
                # Test connection by getting a table reference
                table = connection.table('payments')
                logging.info("Successfully connected to HBase table")
                break
            else:
                logging.error("Table 'payments' does not exist. Please create it using HBase shell")
                raise Exception("Table 'payments' not found")
            
        except Exception as e:
            logging.error(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logging.error("Max retries reached. Could not connect to HBase.")
                raise
        finally:
            if connection:
                connection.close()

if __name__ == "__main__":
    create_table()
