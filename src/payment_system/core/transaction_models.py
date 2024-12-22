from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List

class PaymentType(Enum):
    ACH = "ACH"
    WIRE = "WIRE"
    RTP = "RTP"
    CHECK = "CHECK"
    SEPA = "SEPA_CREDIT_TRANSFER"
    CARD = "CARD_PAYMENT"
    STANDING_ORDER = "STANDING_ORDER"
    P2P = "P2P"
    DIRECT_DEBIT = "DIRECT_DEBIT"

class PaymentStatus(Enum):
    RECEIVED = "RECEIVED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PENDING = "PENDING"
    VALIDATED = "VALIDATED"

@dataclass
class Amount:
    value: float
    currency: str
    exchange_info: Optional[Dict[str, Any]] = None

@dataclass
class RoutingInfo:
    type: str  # ABA, SWIFT, IBAN
    value: str

@dataclass
class AccountInfo:
    account_id: str
    account_type: str
    routing_info: RoutingInfo

@dataclass
class Address:
    street: str
    city: str
    state: Optional[str]
    postal_code: str
    country: str

@dataclass
class ContactInfo:
    address: Address
    electronic: Optional[Dict[str, str]] = None

@dataclass
class Party:
    party_type: str
    party_id: str
    name: str
    account_info: AccountInfo
    contact_info: ContactInfo

@dataclass
class ProcessingInfo:
    priority: str
    processing_window: str
    submission_date: datetime
    effective_date: datetime
    settlement_date: datetime
    status_tracking: Dict[str, Any]

@dataclass
class BasePayment:
    payment_id: str
    payment_type: PaymentType
    version: str
    created_at: datetime
    updated_at: datetime
    status: PaymentStatus
    amount: Amount
    originator: Party
    beneficiary: Party
    processing: ProcessingInfo
    references: Dict[str, Any]
    remittance_info: Dict[str, Any]
    type_specific_data: Dict[str, Any]
    regulatory: Dict[str, Any]
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert the payment object to a dictionary"""
        def _convert(obj):
            if isinstance(obj, (BasePayment, Amount, Party, ProcessingInfo, 
                              RoutingInfo, AccountInfo, Address, ContactInfo)):
                return {k: _convert(v) for k, v in obj.__dict__.items()}
            elif isinstance(obj, (PaymentType, PaymentStatus)):
                return obj.value
            elif isinstance(obj, datetime):
                return obj.isoformat()
            elif isinstance(obj, dict):
                return {k: _convert(v) for k, v in obj.items()}
            elif isinstance(obj, (list, tuple)):
                return [_convert(x) for x in obj]
            else:
                return obj

        return _convert(self)

@dataclass
class ACHPayment(BasePayment):
    sec_code: str
    batch_id: str
    addenda: Optional[Dict[str, Any]] = None

@dataclass
class WirePayment(BasePayment):
    wire_type: str
    message_type: str
    intermediary_bank: Optional[Dict[str, Any]] = None
    charges: Dict[str, Any] = field(default_factory=dict)

@dataclass
class RTPayment(BasePayment):
    clearing_system: str
    settlement_method: str
    confirmation: Dict[str, Any]
    timing: Dict[str, Any]

@dataclass
class CheckPayment(BasePayment):
    check_number: str
    check_type: str
    presentation_type: str
    memo: str
    image_data: Optional[Dict[str, Any]] = None

@dataclass
class SEPAPayment(BasePayment):
    scheme: str
    instruction_id: str
    service_level: str
    local_instrument: str
    category_purpose: str

@dataclass
class CardPayment(BasePayment):
    card_present: bool
    entry_mode: str
    terminal_id: str
    merchant_id: str
    pan_last_4: str
    card_type: str
    network: str
    auth_code: str

@dataclass
class DirectDebitPayment(BasePayment):
    scheme_type: str
    mandate_id: str
    date_of_signature: datetime
    max_amount: float
    frequency: str
    sequence_type: str
    collection_date: datetime
    creditor_id: str

@dataclass
class StandingOrderPayment(BasePayment):
    frequency: str
    execution_day: int
    start_date: datetime
    end_date: datetime
    next_execution: datetime
    previous_execution: datetime
    last_execution_status: str

@dataclass
class P2PPayment(BasePayment):
    service_provider: str
    service_type: str
    purpose: str
    message: str
    device_id: str
    risk_score: int
