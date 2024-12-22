from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import logging
from datetime import date
from .models import (
    PaymentResponse,
    TimeRangeRequest,
    TypeCustomerRequest,
    DateTypeCustomerRequest,
    ErrorResponse
)
from payment_query import PaymentQuery

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Payment Transaction API",
    description="API for querying payment transactions with various filters",
    version="1.0.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency for PaymentQuery
def get_payment_query():
    try:
        query = PaymentQuery()
        return query
    except Exception as e:
        logger.error(f"Failed to connect to HBase: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail="Database connection error"
        )

@app.get("/health")
async def health_check():
    """
    Health check endpoint to verify API is running
    """
    return {"status": "healthy"}

@app.post(
    "/payments/time-range",
    response_model=List[PaymentResponse],
    responses={
        200: {"description": "Successfully retrieved payments"},
        503: {"model": ErrorResponse, "description": "Database connection error"},
    }
)
async def get_payments_by_time_range(
    request: TimeRangeRequest,
    query: PaymentQuery = Depends(get_payment_query)
):
    """
    Retrieve payments within a specified time range.

    - **start_date**: Start date in YYYY-MM-DD format
    - **end_date**: End date in YYYY-MM-DD format
    """
    try:
        payments = query.query_by_time_range(
            str(request.start_date),
            str(request.end_date)
        )
        return [PaymentResponse(**payment) for payment in payments]
    except Exception as e:
        logger.error(f"Error querying payments by time range: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail=str(e)
        )

@app.post(
    "/payments/type-customer",
    response_model=List[PaymentResponse],
    responses={
        200: {"description": "Successfully retrieved payments"},
        503: {"model": ErrorResponse, "description": "Database connection error"},
    }
)
async def get_payments_by_type_and_customer(
    request: TypeCustomerRequest,
    query: PaymentQuery = Depends(get_payment_query)
):
    """
    Retrieve payments by payment type and customer ID.

    - **payment_type**: Type of payment (e.g., WIRE, ACH)
    - **customer_id**: Customer ID
    """
    try:
        payments = query.query_by_type_and_customer(
            request.payment_type,
            request.customer_id
        )
        return [PaymentResponse(**payment) for payment in payments]
    except Exception as e:
        logger.error(f"Error querying payments by type and customer: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail=str(e)
        )

@app.post(
    "/payments/date-type-customer",
    response_model=List[PaymentResponse],
    responses={
        200: {"description": "Successfully retrieved payments"},
        503: {"model": ErrorResponse, "description": "Database connection error"},
    }
)
async def get_payments_by_date_type_customer(
    request: DateTypeCustomerRequest,
    query: PaymentQuery = Depends(get_payment_query)
):
    """
    Retrieve payments by date range, payment type, and customer ID.

    - **start_date**: Start date in YYYY-MM-DD format
    - **end_date**: End date in YYYY-MM-DD format
    - **payment_type**: Type of payment (e.g., WIRE, ACH)
    - **customer_id**: Customer ID
    """
    try:
        payments = query.query_by_date_type_customer(
            str(request.start_date),
            str(request.end_date),
            request.payment_type,
            request.customer_id
        )
        return [PaymentResponse(**payment) for payment in payments]
    except Exception as e:
        logger.error(f"Error querying payments by date, type and customer: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail=str(e)
        )
