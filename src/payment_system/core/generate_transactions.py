from payment_system.core.transaction_generator import TransactionGenerator
import json
from datetime import datetime
import os
import logging
from dataclasses import asdict

# Configure logging to only show warnings and errors
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def generate_payment_by_type(generator, payment_type, count):
    """Generate payments based on payment type"""
    payments = []
    for _ in range(count):
        if payment_type == 'ACH':
            payment = generator.generate_ach_payment()
        elif payment_type == 'WIRE':
            payment = generator.generate_wire_payment()
        elif payment_type == 'CHECK':
            payment = generator.generate_check_payment()
        elif payment_type == 'RTP':
            payment = generator.generate_rtp_payment()
        elif payment_type == 'SEPA':
            payment = generator.generate_sepa_payment()
        elif payment_type == 'CARD':
            payment = generator.generate_card_payment()
        elif payment_type == 'DIRECT_DEBIT':
            payment = generator.generate_direct_debit_payment()
        elif payment_type == 'STANDING_ORDER':
            payment = generator.generate_standing_order_payment()
        elif payment_type == 'P2P':
            payment = generator.generate_p2p_payment()
        else:
            logger.warning(f"Unsupported payment type: {payment_type}")
            continue
        
        # Convert payment object to dictionary using dataclasses.asdict()
        payment_dict = asdict(payment)
        
        # Convert Enum values to strings
        payment_dict['payment_type'] = payment_dict['payment_type'].value
        payment_dict['status'] = payment_dict['status'].value
        
        # Convert datetime objects to ISO format strings
        payment_dict['created_at'] = payment_dict['created_at'].isoformat()
        payment_dict['updated_at'] = payment_dict['updated_at'].isoformat()
        
        if 'processing' in payment_dict:
            if 'submission_date' in payment_dict['processing']:
                payment_dict['processing']['submission_date'] = payment_dict['processing']['submission_date'].isoformat()
            if 'effective_date' in payment_dict['processing']:
                payment_dict['processing']['effective_date'] = payment_dict['processing']['effective_date'].isoformat()
            if 'settlement_date' in payment_dict['processing']:
                payment_dict['processing']['settlement_date'] = payment_dict['processing']['settlement_date'].isoformat()
        
        # Handle specific payment type datetime fields
        if payment_type == 'DIRECT_DEBIT' and 'date_of_signature' in payment_dict:
            payment_dict['date_of_signature'] = payment_dict['date_of_signature'].isoformat()
            payment_dict['collection_date'] = payment_dict['collection_date'].isoformat()
        elif payment_type == 'STANDING_ORDER' and 'start_date' in payment_dict:
            payment_dict['start_date'] = payment_dict['start_date'].isoformat()
            payment_dict['end_date'] = payment_dict['end_date'].isoformat()
            payment_dict['next_execution'] = payment_dict['next_execution'].isoformat()
            payment_dict['previous_execution'] = payment_dict['previous_execution'].isoformat()
        
        payments.append(payment_dict)
    
    return payments

def main():
    try:
        # Create output directory if it doesn't exist
        output_dir = "../../data/generated_transactions"
        os.makedirs(output_dir, exist_ok=True)
        logger.info("Created/verified output directory: generated_transactions")

        # Initialize generator with 1000 B2B customers
        logger.info("Initializing transaction generator with 1000 B2B customers...")
        generator = TransactionGenerator(num_customers=1000)

        # Generate transactions per payment type
        transactions_per_type = 10  # Reduced for initial testing
        
        logger.info(f"Generating {transactions_per_type} transactions per payment type...")
        
        # Generate transactions for each payment type
        transactions = []
        payment_types = ['ACH', 'WIRE', 'CHECK', 'RTP', 'SEPA', 'CARD', 'DIRECT_DEBIT', 'STANDING_ORDER', 'P2P']
        
        for payment_type in payment_types:
            logger.info(f"Generating {payment_type} transactions...")
            batch = generate_payment_by_type(generator, payment_type, transactions_per_type)
            transactions.extend(batch)
            logger.info(f"Generated {len(batch)} {payment_type} transactions")
        
        # Save to JSON with fixed filename
        output_file = os.path.join(output_dir, "transactions.json")
        
        with open(output_file, 'w') as f:
            json.dump({"transactions": transactions}, f, indent=2)
        
        logger.info(f"\nGenerated {len(transactions)} total transactions")
        logger.info(f"Saved to: {output_file}")

        # Print statistics
        logger.info("\nTransaction Statistics:")
        logger.info("-" * 50)
        
        # Count by payment type
        payment_type_counts = {}
        for t in transactions:
            pt = t['payment_type']
            payment_type_counts[pt] = payment_type_counts.get(pt, 0) + 1
        
        logger.info("\nTransactions per payment type:")
        for pt, count in payment_type_counts.items():
            logger.info(f"{pt}: {count}")
        
        # Count by status
        status_counts = {}
        for t in transactions:
            status = t['status']
            status_counts[status] = status_counts.get(status, 0) + 1
        
        logger.info("\nTransaction status distribution:")
        for status, count in status_counts.items():
            logger.info(f"{status}: {count}")
        
        # Amount statistics
        amounts = [t['amount']['value'] for t in transactions]  
        avg_amount = sum(amounts) / len(amounts)
        min_amount = min(amounts)
        max_amount = max(amounts)
        
        logger.info("\nAmount statistics:")
        logger.info(f"Average Amount: ${avg_amount:.2f}")
        logger.info(f"Min Amount: ${min_amount:.2f}")
        logger.info(f"Max Amount: ${max_amount:.2f}")

    except Exception as e:
        logger.error(f"Error generating transactions: {str(e)}")
        raise

if __name__ == "__main__":
    main()
