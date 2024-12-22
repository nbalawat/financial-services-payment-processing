from pydantic import BaseModel, Field
from typing import Optional
from datetime import date
from decimal import Decimal

class PaymentResponse(BaseModel):
    payment_id: str
    payment_type: str
    amount: Decimal
    currency: str
    status: str
    originator_id: str
    originator_name: str
    beneficiary_id: str
    beneficiary_name: str
    created_at: str

class TimeRangeRequest(BaseModel):
    start_date: date = Field(..., description="Start date in YYYY-MM-DD format")
    end_date: date = Field(..., description="End date in YYYY-MM-DD format")

class TypeCustomerRequest(BaseModel):
    payment_type: str = Field(..., description="Type of payment (e.g., WIRE, ACH)")
    customer_id: str = Field(..., description="Customer ID")

class DateTypeCustomerRequest(BaseModel):
    start_date: date = Field(..., description="Start date in YYYY-MM-DD format")
    end_date: date = Field(..., description="End date in YYYY-MM-DD format")
    payment_type: str = Field(..., description="Type of payment (e.g., WIRE, ACH)")
    customer_id: str = Field(..., description="Customer ID")

class ErrorResponse(BaseModel):
    error: str
    details: Optional[str] = None
