import happybase
import logging

logging.basicConfig(level=logging.INFO)

def scan_payments_table():
    try:
        # Connect to HBase
        connection = happybase.Connection('localhost', port=9091, protocol='binary', transport='buffered')
        table = connection.table('payments')
        
        # Scan the table
        logging.info("Scanning 'payments' table...")
        count = 0
        for key, data in table.scan():
            count += 1
            logging.info(f"\nRow key: {key.decode('utf-8')}")
            for column, value in data.items():
                col_family, col_name = column.decode('utf-8').split(':')
                logging.info(f"{col_family}:{col_name} = {value.decode('utf-8')}")
        
        logging.info(f"\nTotal records found: {count}")
            
    except Exception as e:
        logging.error(f"Error scanning table: {str(e)}")
    finally:
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    scan_payments_table()
