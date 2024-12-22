from faker import Faker
import random
from datetime import datetime, timedelta
import uuid
from decimal import Decimal
import logging
import pandas as pd
from typing import List, Dict, Any

from payment_system.core.transaction_models import (
    PaymentType, PaymentStatus, BasePayment,
    ACHPayment, WirePayment, CheckPayment,
    RTPayment, SEPAPayment, CardPayment,
    DirectDebitPayment, StandingOrderPayment,
    P2PPayment, Amount, Party, AccountInfo,
    RoutingInfo, Address, ContactInfo,
    ProcessingInfo
)

class TransactionGenerator:
    def __init__(self, num_customers: int = 100):
        self.fake = Faker()
        self.customers = self._generate_customers(num_customers)
        self.banks = self._generate_banks()
        
    def _generate_customers(self, num_customers: int) -> List[Dict[str, Any]]:
        """Generate a list of fake B2B customers"""
        customers = []
        for _ in range(num_customers):
            customers.append({
                'party_id': f'CUST{uuid.uuid4().hex[:8].upper()}',
                'name': self.fake.company(),
                'party_type': 'CORPORATE',
                'account_info': {
                    'account_id': f'ACC{self.fake.random_number(digits=10, fix_len=True)}',
                    'account_type': random.choice(['CHECKING', 'SAVINGS']),
                    'routing_info': {
                        'type': 'ABA',
                        'value': f'{self.fake.random_number(digits=9, fix_len=True)}'
                    }
                },
                'contact_info': {
                    'address': {
                        'street': self.fake.street_address(),
                        'city': self.fake.city(),
                        'state': self.fake.state(),
                        'postal_code': self.fake.postcode(),
                        'country': 'US'
                    },
                    'electronic': {
                        'email': self.fake.company_email(),
                        'phone': self.fake.phone_number()
                    }
                }
            })
        return customers

    def _generate_banks(self, num_banks: int = 50) -> List[Dict[str, Any]]:
        """Generate a list of fake banks"""
        banks = []
        for _ in range(num_banks):
            banks.append({
                'name': f"{self.fake.company()} Bank",
                'swift': f"SWIFT{self.fake.random_number(digits=8, fix_len=True)}",
                'routing': f"{self.fake.random_number(digits=9, fix_len=True)}",
                'address': {
                    'street': self.fake.street_address(),
                    'city': self.fake.city(),
                    'state': self.fake.state(),
                    'postal_code': self.fake.postcode(),
                    'country': 'US'
                }
            })
        return banks

    def _create_party(self, customer: Dict[str, Any], bank: Dict[str, Any]) -> Party:
        """Create a party object from customer and bank info"""
        return Party(
            party_type=customer['party_type'],
            party_id=customer['party_id'],
            name=customer['name'],
            account_info=AccountInfo(
                account_id=customer['account_info']['account_id'],
                account_type=customer['account_info']['account_type'],
                routing_info=RoutingInfo(
                    type=customer['account_info']['routing_info']['type'],
                    value=customer['account_info']['routing_info']['value']
                )
            ),
            contact_info=ContactInfo(
                address=Address(
                    street=customer['contact_info']['address']['street'],
                    city=customer['contact_info']['address']['city'],
                    state=customer['contact_info']['address']['state'],
                    postal_code=customer['contact_info']['address']['postal_code'],
                    country=customer['contact_info']['address']['country']
                ),
                electronic=customer['contact_info']['electronic']
            )
        )

    def _create_processing_info(self) -> ProcessingInfo:
        """Create processing info with random dates"""
        submission_date = datetime.now()
        effective_date = submission_date + timedelta(days=random.randint(0, 2))
        settlement_date = effective_date + timedelta(days=random.randint(0, 1))
        
        return ProcessingInfo(
            priority=random.choice(['HIGH', 'NORMAL', 'LOW']),
            processing_window=random.choice(['SAME_DAY', 'NEXT_DAY']),
            submission_date=submission_date,
            effective_date=effective_date,
            settlement_date=settlement_date,
            status_tracking={
                'current_status': random.choice([s.value for s in PaymentStatus]),
                'status_history': []
            }
        )

    def generate_ach_payment(self) -> ACHPayment:
        """Generate an ACH payment"""
        originator = random.choice(self.customers)
        beneficiary = random.choice(self.customers)
        while originator == beneficiary:
            beneficiary = random.choice(self.customers)

        return ACHPayment(
            payment_id=f"ACH-{uuid.uuid4().hex[:8].upper()}",
            payment_type=PaymentType.ACH,
            version="1.0",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            status=random.choice(list(PaymentStatus)),
            amount=Amount(
                value=round(random.uniform(100, 1000000), 2),
                currency="USD"
            ),
            originator=self._create_party(originator, random.choice(self.banks)),
            beneficiary=self._create_party(beneficiary, random.choice(self.banks)),
            processing=self._create_processing_info(),
            references={
                'end_to_end_id': f"E2E{uuid.uuid4().hex[:8].upper()}",
                'transaction_id': f"TXN{uuid.uuid4().hex[:8].upper()}"
            },
            remittance_info={
                'type': 'STRUCTURED',
                'purpose_code': 'SUPP',
                'description': f"Invoice {self.fake.random_number(digits=6, fix_len=True)}"
            },
            type_specific_data={
                'sec_code': random.choice(['CCD', 'CTX', 'PPD']),
                'batch_data': {},
                'addenda': []
            },
            regulatory={
                'compliance_checks': {
                    'sanctions_check': {'status': 'CLEARED'},
                    'aml_check': {'status': 'CLEARED'}
                }
            },
            metadata={
                'source_system': 'PAYMENT_SYSTEM',
                'business_unit': 'TREASURY'
            },
            sec_code=random.choice(['CCD', 'CTX', 'PPD']),
            batch_id=f"BATCH{uuid.uuid4().hex[:8].upper()}",
            addenda={
                'type_code': '05',
                'payment_related_info': f"Invoice {self.fake.random_number(digits=6, fix_len=True)}"
            }
        )

    def generate_wire_payment(self) -> WirePayment:
        """Generate a wire payment"""
        originator = random.choice(self.customers)
        beneficiary = random.choice(self.customers)
        while originator == beneficiary:
            beneficiary = random.choice(self.customers)

        return WirePayment(
            payment_id=f"WIRE-{uuid.uuid4().hex[:8].upper()}",
            payment_type=PaymentType.WIRE,
            version="1.0",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            status=random.choice(list(PaymentStatus)),
            amount=Amount(
                value=round(random.uniform(10000, 10000000), 2),
                currency="USD"
            ),
            originator=self._create_party(originator, random.choice(self.banks)),
            beneficiary=self._create_party(beneficiary, random.choice(self.banks)),
            processing=self._create_processing_info(),
            references={
                'sender_reference': f"REF{uuid.uuid4().hex[:8].upper()}",
                'beneficiary_reference': f"BREF{uuid.uuid4().hex[:8].upper()}"
            },
            remittance_info={
                'type': 'STRUCTURED',
                'purpose_code': 'TRADE'
            },
            type_specific_data={
                'message_type': 'MT103',
                'charges_instruction': random.choice(['OUR', 'BEN', 'SHA'])
            },
            regulatory={
                'compliance_checks': {
                    'sanctions_check': {'status': 'CLEARED'},
                    'aml_check': {'status': 'CLEARED'},
                    'ofac_check': {'status': 'CLEARED'}
                }
            },
            metadata={
                'source_system': 'WIRE_SYSTEM',
                'business_unit': 'TREASURY'
            },
            wire_type=random.choice(['DOMESTIC', 'INTERNATIONAL']),
            message_type='MT103',
            charges={'type': random.choice(['OUR', 'BEN', 'SHA']), 'amount': 45.00}
        )

    def generate_rtp_payment(self) -> RTPayment:
        """Generate a Real-Time Payment transaction"""
        originator = random.choice(self.customers)
        beneficiary = random.choice(self.customers)
        
        # RTP transactions are typically settled within seconds
        submission_time = datetime.now()
        settlement_time = submission_time + timedelta(seconds=random.randint(1, 5))
        
        return RTPayment(
            payment_id=f"RTP-{uuid.uuid4().hex[:8].upper()}",
            payment_type=PaymentType.RTP,
            version="1.0",
            created_at=submission_time,
            updated_at=submission_time,
            status=PaymentStatus.RECEIVED,
            amount=Amount(
                value=round(random.uniform(100, 50000), 2),
                currency="USD"
            ),
            originator=originator,
            beneficiary=beneficiary,
            processing=ProcessingInfo(
                priority="HIGH",
                processing_window="REAL_TIME",
                submission_date=submission_time,
                effective_date=submission_time,
                settlement_date=settlement_time,
                status_tracking={}
            ),
            references={},
            remittance_info={},
            type_specific_data={},
            regulatory={},
            metadata={},
            clearing_system="TCH-RTP",
            settlement_method="REAL_TIME_GROSS_SETTLEMENT",
            confirmation={
                "required": True,
                "window_minutes": 1
            },
            timing={
                "submission_time": submission_time.isoformat(),
                "expected_settlement_time": settlement_time.isoformat()
            }
        )

    def generate_check_payment(self) -> CheckPayment:
        """Generate a Check payment transaction"""
        originator = random.choice(self.customers)
        beneficiary = random.choice(self.customers)
        while originator == beneficiary:
            beneficiary = random.choice(self.customers)

        check_number = str(random.randint(1000, 9999))
        
        return CheckPayment(
            payment_id=f"CHECK-{uuid.uuid4().hex[:8].upper()}",
            payment_type=PaymentType.CHECK,
            version="1.0",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            status=random.choice(list(PaymentStatus)),
            amount=Amount(
                value=round(random.uniform(100, 50000), 2),
                currency="USD"
            ),
            originator=self._create_party(originator, random.choice(self.banks)),
            beneficiary=self._create_party(beneficiary, random.choice(self.banks)),
            processing=self._create_processing_info(),
            references={
                'check_number': check_number,
                'transaction_id': f"TXN{uuid.uuid4().hex[:8].upper()}"
            },
            remittance_info={
                'type': 'STRUCTURED',
                'purpose_code': 'SUPP',
                'description': f"Check payment {check_number}"
            },
            type_specific_data={
                'check_type': 'BUSINESS',
                'presentation_type': random.choice(['PAPER', 'IMAGE']),
                'capture_type': 'REMOTE_DEPOSIT'
            },
            regulatory={
                'compliance_checks': {
                    'sanctions_check': {'status': 'CLEARED'},
                    'aml_check': {'status': 'CLEARED'}
                }
            },
            metadata={
                'source_system': 'CHECK_SYSTEM',
                'business_unit': 'TREASURY'
            },
            check_number=check_number,
            check_type='BUSINESS',
            presentation_type=random.choice(['PAPER', 'IMAGE']),
            memo=f"Payment for Invoice {self.fake.random_number(digits=6, fix_len=True)}"
        )

    def generate_sepa_payment(self) -> SEPAPayment:
        """Generate a SEPA payment transaction"""
        originator = random.choice(self.customers)
        beneficiary = random.choice(self.customers)
        while originator == beneficiary:
            beneficiary = random.choice(self.customers)

        return SEPAPayment(
            payment_id=f"SEPA-{uuid.uuid4().hex[:8].upper()}",
            payment_type=PaymentType.SEPA,
            version="1.0",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            status=random.choice(list(PaymentStatus)),
            amount=Amount(
                value=round(random.uniform(100, 100000), 2),
                currency="EUR"  # SEPA is always in EUR
            ),
            originator=self._create_party(originator, random.choice(self.banks)),
            beneficiary=self._create_party(beneficiary, random.choice(self.banks)),
            processing=self._create_processing_info(),
            references={
                'end_to_end_id': f"E2E{uuid.uuid4().hex[:8].upper()}",
                'instruction_id': f"INST{uuid.uuid4().hex[:8].upper()}"
            },
            remittance_info={
                'type': 'STRUCTURED',
                'purpose_code': 'SUPP',
                'creditor_reference': f"RF{self.fake.random_number(digits=8, fix_len=True)}"
            },
            type_specific_data={
                'scheme': 'SCT_INST',
                'service_level': 'SEPA',
                'local_instrument': 'INST'
            },
            regulatory={
                'compliance_checks': {
                    'sanctions_check': {'status': 'CLEARED'},
                    'aml_check': {'status': 'CLEARED'}
                },
                'reporting': {
                    'authority_name': 'EUROPEAN_CENTRAL_BANK',
                    'details': 'STATISTICAL_REPORTING'
                }
            },
            metadata={
                'source_system': 'SEPA_SYSTEM',
                'business_unit': 'EUROPEAN_PAYMENTS'
            },
            scheme='SCT_INST',
            instruction_id=f"INST{uuid.uuid4().hex[:8].upper()}",
            service_level='SEPA',
            local_instrument='INST',
            category_purpose='SUPP'
        )

    def generate_card_payment(self) -> CardPayment:
        """Generate a Card payment transaction"""
        originator = random.choice(self.customers)
        beneficiary = random.choice(self.customers)
        while originator == beneficiary:
            beneficiary = random.choice(self.customers)

        return CardPayment(
            payment_id=f"CARD-{uuid.uuid4().hex[:8].upper()}",
            payment_type=PaymentType.CARD,
            version="1.0",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            status=random.choice(list(PaymentStatus)),
            amount=Amount(
                value=round(random.uniform(10, 5000), 2),
                currency="USD"
            ),
            originator=self._create_party(originator, random.choice(self.banks)),
            beneficiary=self._create_party(beneficiary, random.choice(self.banks)),
            processing=self._create_processing_info(),
            references={
                'transaction_id': f"TXN{uuid.uuid4().hex[:8].upper()}"
            },
            remittance_info={
                'type': 'STRUCTURED',
                'purpose_code': 'RETAIL',
                'description': 'Card Payment'
            },
            type_specific_data={
                'card_present': True,
                'entry_mode': random.choice(['CHIP', 'CONTACTLESS', 'MANUAL']),
                'auth_type': 'ONLINE'
            },
            regulatory={
                'compliance_checks': {
                    'sanctions_check': {'status': 'CLEARED'},
                    'fraud_check': {'status': 'CLEARED'}
                }
            },
            metadata={
                'source_system': 'CARD_SYSTEM',
                'business_unit': 'MERCHANT_SERVICES'
            },
            card_present=True,
            entry_mode=random.choice(['CHIP', 'CONTACTLESS', 'MANUAL']),
            terminal_id=f"TERM{self.fake.random_number(digits=6, fix_len=True)}",
            merchant_id=f"MERCH{self.fake.random_number(digits=6, fix_len=True)}",
            pan_last_4=str(random.randint(1000, 9999)),
            card_type=random.choice(['CREDIT', 'DEBIT']),
            network=random.choice(['VISA', 'MASTERCARD', 'AMEX']),
            auth_code=f"AUTH{self.fake.random_number(digits=6, fix_len=True)}"
        )

    def generate_direct_debit_payment(self) -> DirectDebitPayment:
        """Generate a Direct Debit payment transaction"""
        originator = random.choice(self.customers)
        beneficiary = random.choice(self.customers)
        while originator == beneficiary:
            beneficiary = random.choice(self.customers)

        start_date = datetime.now()
        collection_date = start_date + timedelta(days=random.randint(1, 30))

        return DirectDebitPayment(
            payment_id=f"DD-{uuid.uuid4().hex[:8].upper()}",
            payment_type=PaymentType.DIRECT_DEBIT,
            version="1.0",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            status=random.choice(list(PaymentStatus)),
            amount=Amount(
                value=round(random.uniform(10, 5000), 2),
                currency="USD"
            ),
            originator=self._create_party(originator, random.choice(self.banks)),
            beneficiary=self._create_party(beneficiary, random.choice(self.banks)),
            processing=self._create_processing_info(),
            references={
                'mandate_id': f"MNDT{uuid.uuid4().hex[:8].upper()}",
                'transaction_id': f"TXN{uuid.uuid4().hex[:8].upper()}"
            },
            remittance_info={
                'type': 'STRUCTURED',
                'purpose_code': 'RECURRING',
                'description': 'Direct Debit Payment'
            },
            type_specific_data={
                'scheme_type': 'SEPA_DIRECT_DEBIT',
                'recurring_type': 'RCUR'
            },
            regulatory={
                'compliance_checks': {
                    'sanctions_check': {'status': 'CLEARED'},
                    'mandate_check': {'status': 'VALID'}
                }
            },
            metadata={
                'source_system': 'DD_SYSTEM',
                'business_unit': 'COLLECTIONS'
            },
            scheme_type='SEPA_DIRECT_DEBIT',
            mandate_id=f"MNDT{uuid.uuid4().hex[:8].upper()}",
            date_of_signature=start_date,
            max_amount=5000.00,
            frequency='MONTHLY',
            sequence_type='RCUR',
            collection_date=collection_date,
            creditor_id=f"CRED{uuid.uuid4().hex[:8].upper()}"
        )

    def generate_standing_order_payment(self) -> StandingOrderPayment:
        """Generate a Standing Order payment transaction"""
        originator = random.choice(self.customers)
        beneficiary = random.choice(self.customers)
        while originator == beneficiary:
            beneficiary = random.choice(self.customers)

        start_date = datetime.now()
        end_date = start_date + timedelta(days=365)
        previous_execution = start_date - timedelta(days=30)
        next_execution = start_date + timedelta(days=30)

        return StandingOrderPayment(
            payment_id=f"SO-{uuid.uuid4().hex[:8].upper()}",
            payment_type=PaymentType.STANDING_ORDER,
            version="1.0",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            status=random.choice(list(PaymentStatus)),
            amount=Amount(
                value=round(random.uniform(100, 5000), 2),
                currency="USD"
            ),
            originator=self._create_party(originator, random.choice(self.banks)),
            beneficiary=self._create_party(beneficiary, random.choice(self.banks)),
            processing=self._create_processing_info(),
            references={
                'standing_order_id': f"SO{uuid.uuid4().hex[:8].upper()}",
                'transaction_id': f"TXN{uuid.uuid4().hex[:8].upper()}"
            },
            remittance_info={
                'type': 'STRUCTURED',
                'purpose_code': 'RECURRING',
                'description': 'Standing Order Payment'
            },
            type_specific_data={
                'frequency': 'MONTHLY',
                'execution_day': 15
            },
            regulatory={
                'compliance_checks': {
                    'sanctions_check': {'status': 'CLEARED'}
                }
            },
            metadata={
                'source_system': 'SO_SYSTEM',
                'business_unit': 'RECURRING_PAYMENTS'
            },
            frequency='MONTHLY',
            execution_day=15,
            start_date=start_date,
            end_date=end_date,
            next_execution=next_execution,
            previous_execution=previous_execution,
            last_execution_status='SUCCESS'
        )

    def generate_p2p_payment(self) -> P2PPayment:
        """Generate a P2P payment transaction"""
        originator = random.choice(self.customers)
        beneficiary = random.choice(self.customers)
        while originator == beneficiary:
            beneficiary = random.choice(self.customers)

        return P2PPayment(
            payment_id=f"P2P-{uuid.uuid4().hex[:8].upper()}",
            payment_type=PaymentType.P2P,
            version="1.0",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            status=random.choice(list(PaymentStatus)),
            amount=Amount(
                value=round(random.uniform(10, 1000), 2),
                currency="USD"
            ),
            originator=self._create_party(originator, random.choice(self.banks)),
            beneficiary=self._create_party(beneficiary, random.choice(self.banks)),
            processing=self._create_processing_info(),
            references={
                'transaction_id': f"TXN{uuid.uuid4().hex[:8].upper()}"
            },
            remittance_info={
                'type': 'UNSTRUCTURED',
                'description': self.fake.text(max_nb_chars=50)
            },
            type_specific_data={
                'service_provider': 'ZELLE',
                'service_type': 'INSTANT'
            },
            regulatory={
                'compliance_checks': {
                    'sanctions_check': {'status': 'CLEARED'},
                    'fraud_check': {'status': 'CLEARED'}
                }
            },
            metadata={
                'source_system': 'P2P_SYSTEM',
                'business_unit': 'DIGITAL_PAYMENTS'
            },
            service_provider='ZELLE',
            service_type='INSTANT',
            purpose='PAYMENT',
            message=self.fake.text(max_nb_chars=50),
            device_id=f"DEV{uuid.uuid4().hex[:8].upper()}",
            risk_score=random.randint(0, 100)
        )

    def generate_transactions(self, num_transactions: int, payment_type: PaymentType) -> List[BasePayment]:
        """Generate a specified number of transactions of a given payment type"""
        transactions = []
        
        for _ in range(num_transactions):
            if payment_type == PaymentType.ACH:
                transaction = self.generate_ach_payment()
            elif payment_type == PaymentType.WIRE:
                transaction = self.generate_wire_payment()
            elif payment_type == PaymentType.RTP:
                transaction = self.generate_rtp_payment()
            elif payment_type == PaymentType.CHECK:
                transaction = self.generate_check_payment()
            elif payment_type == PaymentType.SEPA:
                transaction = self.generate_sepa_payment()
            elif payment_type == PaymentType.CARD:
                transaction = self.generate_card_payment()
            elif payment_type == PaymentType.DIRECT_DEBIT:
                transaction = self.generate_direct_debit_payment()
            elif payment_type == PaymentType.STANDING_ORDER:
                transaction = self.generate_standing_order_payment()
            elif payment_type == PaymentType.P2P:
                transaction = self.generate_p2p_payment()
            else:
                raise ValueError(f"Payment type {payment_type} not implemented yet")
                
            transactions.append(transaction)

        return transactions

    def generate_all_payment_types(self, transactions_per_type: int) -> pd.DataFrame:
        """Generate transactions for all implemented payment types and return as a DataFrame"""
        all_transactions = []
        
        implemented_types = [
            PaymentType.ACH,
            PaymentType.WIRE,
            PaymentType.RTP,
            PaymentType.CHECK,
            PaymentType.SEPA,
            PaymentType.CARD,
            PaymentType.DIRECT_DEBIT,
            PaymentType.STANDING_ORDER,
            PaymentType.P2P
        ]
        
        for payment_type in implemented_types:
            print(f"Generating {transactions_per_type} {payment_type.value} transactions...")
            transactions = self.generate_transactions(transactions_per_type, payment_type)
            all_transactions.extend([self._transaction_to_dict(t) for t in transactions])
        
        return pd.DataFrame(all_transactions)

    def _transaction_to_dict(self, transaction: BasePayment) -> Dict[str, Any]:
        """Convert a transaction object to a dictionary format"""
        base_dict = {
            "payment_id": transaction.payment_id,
            "payment_type": transaction.payment_type.value,
            "version": transaction.version,
            "created_at": transaction.created_at.isoformat(),
            "updated_at": transaction.updated_at.isoformat(),
            "status": transaction.status.value,
            "amount": transaction.amount.value,
            "currency": transaction.amount.currency,
            "originator_id": transaction.originator.party_id,
            "originator_name": transaction.originator.name,
            "beneficiary_id": transaction.beneficiary.party_id,
            "beneficiary_name": transaction.beneficiary.name
        }

        # Add type-specific fields
        if isinstance(transaction, ACHPayment):
            base_dict.update({
                "sec_code": transaction.sec_code,
                "batch_id": transaction.batch_id
            })
        elif isinstance(transaction, WirePayment):
            base_dict.update({
                "wire_type": transaction.wire_type,
                "message_type": transaction.message_type
            })
        elif isinstance(transaction, RTPayment):
            base_dict.update({
                "clearing_system": transaction.clearing_system,
                "settlement_method": transaction.settlement_method
            })
        elif isinstance(transaction, CheckPayment):
            base_dict.update({
                "check_number": transaction.check_number,
                "check_type": transaction.check_type,
                "presentation_type": transaction.presentation_type
            })
        elif isinstance(transaction, SEPAPayment):
            base_dict.update({
                "scheme": transaction.scheme,
                "service_level": transaction.service_level,
                "local_instrument": transaction.local_instrument
            })
        elif isinstance(transaction, CardPayment):
            base_dict.update({
                "card_type": transaction.card_type,
                "network": transaction.network,
                "entry_mode": transaction.entry_mode
            })
        elif isinstance(transaction, DirectDebitPayment):
            base_dict.update({
                "scheme_type": transaction.scheme_type,
                "mandate_id": transaction.mandate_id,
                "frequency": transaction.frequency
            })
        elif isinstance(transaction, StandingOrderPayment):
            base_dict.update({
                "frequency": transaction.frequency,
                "execution_day": transaction.execution_day,
                "next_execution": transaction.next_execution.isoformat()
            })
        elif isinstance(transaction, P2PPayment):
            base_dict.update({
                "service_provider": transaction.service_provider,
                "service_type": transaction.service_type,
                "risk_score": transaction.risk_score
            })

        return base_dict
