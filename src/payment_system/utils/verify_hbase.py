import happybase
import logging

logging.basicConfig(level=logging.INFO)

def verify_hbase_data():
    connection = None
    try:
        # Connect to HBase using the correct port (9091)
        logging.info("Connecting to HBase on port 9091...")
        connection = happybase.Connection('localhost', port=9091, protocol='binary', transport='buffered')
        table = connection.table('payments')
        
        # Scan the table
        logging.info("Scanning HBase table 'payments'...")
        count = 0
        for key, data in table.scan():
            count += 1
            logging.info(f"\nRow {count}:")
            logging.info(f"Key: {key.decode('utf-8')}")
            for column, value in data.items():
                logging.info(f"{column.decode('utf-8')}: {value.decode('utf-8')}")
        
        logging.info(f"\nTotal records found: {count}")
        
    except Exception as e:
        logging.error(f"Error scanning table: {str(e)}")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    verify_hbase_data()
