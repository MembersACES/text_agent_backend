from dotenv import load_dotenv
load_dotenv()
from fastapi import FastAPI, Depends, HTTPException, Query, UploadFile, File, Form, status
from typing import List
from fastapi import Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi import UploadFile, File, Form
from pydantic import BaseModel
from google.oauth2 import id_token
from google.auth.transport import requests as grequests
from google.oauth2.service_account import Credentials as ServiceCredentials
from googleapiclient.discovery import build as google_build
import tempfile
import requests
import copy
from google.auth.transport.requests import Request
from urllib.parse import urlparse, parse_qs
import os
import logging
import tempfile
import os
import httpx
from typing import Optional, List, Dict, Union
import json
from fastapi import HTTPException, Depends
from fastapi.responses import JSONResponse, Response
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import uuid
import google.auth
import csv
import io

# Adjust this import if your function is in a different location
from tools.business_info import get_business_information, get_base1_landing_responses
from services import airtable_client
from tools.get_electricity_ci_latest_invoice_information import get_electricity_ci_latest_invoice_information
from tools.get_electricity_sme_latest_invoice_information import get_electricity_sme_latest_invoice_information
from tools.get_gas_latest_invoice_information import get_gas_latest_invoice_information
from tools.get_gas_sme_latest_invoice_information import get_gas_sme_latest_invoice_information
from tools.get_waste_latest_invoice_information import get_waste_latest_invoice_information
from tools.get_oil_invoice_information import get_oil_invoice_information
from tools.get_cleaning_invoice_information import get_cleaning_invoice_information
from tools.supplier_data_request import supplier_data_request
from tools.drive_filing import drive_filing
from tools.send_supplier_signed_agreement import send_supplier_signed_agreement
from tools.document_generation import (
    loa_generation,
    service_fee_agreement_generation,
    expression_of_interest_generation,
    ghg_offer_generation,
    get_available_eoi_types,
    engagement_form_generation,
    get_available_engagement_form_types,
    generate_testimonial_document,
)
from tools.supplier_quote_request import send_supplier_quote_request
from tools.loa_generation import loa_generation_new
from tools.service_agreement_generation import service_agreement_generation_new
from tools.send_supplier_signed_agreement import send_supplier_signed_agreement_multiple
from tools.one_month_savings import (
    log_invoice_to_sheets,
    get_invoice_history,
    update_invoice_status,
    get_next_sequential_invoice_number,
    get_or_create_subfolder,
    upload_pdf_to_drive,
    upload_file_to_drive,
    extract_folder_id_from_url,
    get_drive_service,
)
from tools.one_month_savings_calculation import calculate_one_month_savings
from tools.contract_ending_sheet import sync_contract_end_dates_to_airtable
from tools.testimonial_solution_content import get_merged_content, save_override
from tools.testimonial_examples import get_testimonials_for_solution_type

# Database imports
from database import get_db, init_db
from models import (
    Task,
    User,
    TaskHistory,
    ClientStatusNote,
    Client,
    ClientReferral,
    Offer,
    OfferActivity,
    StrategyItem,
    Testimonial,
)
from schemas import (
    TaskCreate,
    TaskUpdate,
    TaskStatusUpdate,
    TaskResponse,
    UserResponse,
    ClientStatusNoteCreate,
    ClientStatusNoteUpdate,
    ClientStatusNoteResponse,
    ClientCreate,
    ClientUpdate,
    ClientResponse,
    ClientReferralCreate,
    ClientReferralUpdate,
    ClientReferralResponse,
    OfferCreate,
    OfferUpdate,
    OfferResponse,
    OfferActivityCreate,
    OfferActivityResponse,
    ActivityReportItem,
    OfferPipelineStage as OfferPipelineStageSchema,
    StrategyItemCreate,
    StrategyItemUpdate,
    StrategyItemResponse,
    TestimonialResponse,
    TestimonialUpdate,
    TestimonialCheckApprovedResponse,
    TestimonialSolutionContentItem,
    TestimonialSolutionContentUpdate,
)
from crm_enums import (
    ClientStage,
    OfferStatus,
    OfferActivityType,
    OfferPipelineStage,
    POST_WIN_STAGES,
)
from services.crm import (
    upsert_client_from_business_info,
    update_client_stage_with_history,
    update_offer_status_and_propagate_client_stage,
    create_offer_activity,
    get_or_create_offer_for_activity,
    sync_strategy_status_from_offer,
    sync_strategy_items_from_crm,
)
from sqlalchemy.orm import Session
from sqlalchemy import func, or_
from utils.task_history import (
    log_task_created,
    log_field_change,
    log_status_change,
    log_task_deleted,
)

# Email and scheduler imports
from email_service import send_new_task_email, send_task_completed_email, check_due_tasks
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, date

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

app = FastAPI()


@app.on_event("startup")
def on_startup() -> None:
    """
    Ensure all SQLAlchemy models (including new CRM tables like 'clients' and 'offers')
    are created in the configured database on application startup.
    """
    init_db()

# CORS: allow these origins so error responses (e.g. 500) can include CORS headers
CORS_ORIGINS = [
    "https://acesagentinterface-672026052958.australia-southeast2.run.app",
    "https://acesagentinterfacedev-672026052958.australia-southeast2.run.app",
    "https://acesagentinterface-672026052958.australia-southeast7.run.app",
    "https://acesagentinterfacedev-672026052958.australia-southeast7.run.app",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "https://script.google.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from fastapi import Header
from starlette.requests import Request as StarletteRequest

def _cors_headers_for_origin(origin: Optional[str]) -> Dict[str, str]:
    """Return CORS headers for a response if origin is allowed (so 500/error responses still allow CORS)."""
    if not origin or origin not in CORS_ORIGINS:
        return {}
    return {
        "Access-Control-Allow-Origin": origin,
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Allow-Methods": "*",
        "Access-Control-Allow-Headers": "*",
    }

@app.exception_handler(Exception)
async def global_exception_handler(request: StarletteRequest, exc: Exception):
    """Ensure every response (including 500 and 4xx) includes CORS headers."""
    origin = request.headers.get("origin")
    headers = _cors_headers_for_origin(origin)
    if isinstance(exc, HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
            headers=headers,
        )
    logging.exception("Unhandled exception: %s", exc)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
        headers=headers,
    )

@app.get("/")
async def root():
    return {"status": "healthy", "service": "text-agent-backend"}

@app.get("/health")
async def health_check():
    return {"status": "ok"}
    
def verify_google_access_token(authorization: str = Header(...)):
    """Verify Google access token for API access (needed for presentations)"""
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    
    access_token = authorization.split("Bearer ")[1]
    
    try:
        credentials = Credentials(token=access_token)
        # Test with a minimal API call
        service = build('drive', 'v3', credentials=credentials)
        service.files().list(pageSize=1).execute()
        
        return {"access_token": access_token}
    except Exception as e:
        error_msg = str(e).lower()
        logging.error(f"Access token verification failed: {e}")
        
        # Be more restrictive - only trigger reauthentication for actual token expiry
        if "token_expired" in error_msg or "expired_token" in error_msg:
            raise HTTPException(
                status_code=401,
                detail="REAUTHENTICATION_REQUIRED"
            )
        
        # For other errors, don't trigger reauthentication
        raise HTTPException(status_code=401, detail="Token validation failed")
        
def verify_google_token(authorization: str = Header(...)):
    """Verify Google ID token for basic user authentication"""
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    
    token = authorization.split("Bearer ")[1]
    
    try:
        # Use ID token verification for basic auth (no API access needed)
        idinfo = id_token.verify_oauth2_token(token, grequests.Request(), GOOGLE_CLIENT_ID)
        logging.info(f"Token verified for user: {idinfo.get('email')}")
        return idinfo
    except ValueError as e:
        error_msg = str(e).lower()
        logging.error(f"Token verification failed: {e}")
        
        if "expired" in error_msg:
            raise HTTPException(status_code=401, detail="REAUTHENTICATION_REQUIRED")
        
        raise HTTPException(status_code=401, detail="Invalid token")
    except Exception as e:
        logging.error(f"Token verification error: {e}")
        raise HTTPException(status_code=401, detail="Invalid token")

# Optional: Access token verification (only if you need Google API access)
def verify_google_access_token_optional(authorization: str = Header(...)):
    """Verify Google access token - only use if you need API access"""
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    
    access_token = authorization.split("Bearer ")[1]
    
    try:
        credentials = Credentials(token=access_token)
        # Test with a minimal API call
        service = build('drive', 'v3', credentials=credentials)
        service.files().list(pageSize=1).execute()
        
        return {"access_token": access_token}
    except Exception as e:
        error_msg = str(e).lower()
        logging.error(f"Access token verification failed: {e}")
        
        # Be more specific about when to trigger reauthentication
        if any(phrase in error_msg.lower() for phrase in [
            "token_expired",
            "signature has expired", 
            "token has expired"
        ]):
            raise HTTPException(
                status_code=401,
                detail="REAUTHENTICATION_REQUIRED"
            )
        
        raise HTTPException(status_code=401, detail="Invalid access token")

# Helper function to get or create user
def get_or_create_user(db: Session, email: str, name: str = None, picture: str = None):
    """Get or create a user in the database"""
    user = db.query(User).filter(User.email == email).first()
    if not user:
        user = User(email=email, name=name, picture=picture)
        db.add(user)
        db.commit()
        db.refresh(user)
        logging.info(f"Created new user: {email}")
    return user

# Recommended: Use this for most endpoints
def get_current_user(authorization: str = Header(...)):
    """Get current authenticated user info"""
    return verify_google_token(authorization)

# New dependency that verifies token AND ensures user exists in database
def get_current_user_with_db(
    authorization: str = Header(...),
    db: Session = Depends(get_db)
):
    """Get current authenticated user info and ensure they exist in database"""
    idinfo = verify_google_token(authorization)
    email = idinfo.get("email")
    name = idinfo.get("name")
    picture = idinfo.get("picture")
    
    # Auto-create user if they don't exist
    user = get_or_create_user(db, email, name, picture)
    
    # Return both the idinfo (for compatibility) and user object
    return {"idinfo": idinfo, "user": user}

class BusinessInfoRequest(BaseModel):
    business_name: str

class DocumentGenerationRequest(BaseModel):
    business_name: str
    abn: str
    trading_as: str
    postal_address: str
    site_address: str
    telephone: str
    email: str
    contact_name: str
    position: str
    client_folder_url: str

class NewLOAGeneration(BaseModel):
    business_name: str
    abn: str
    trading_as: str
    postal_address: str
    site_address: str
    telephone: str
    email: str
    contact_name: str
    position: str

class EOIGenerationRequest(DocumentGenerationRequest):
    expression_type: str

class EngagementFormGenerationRequest(DocumentGenerationRequest):
    engagement_form_type: str

class UtilityInfoRequest(BaseModel):
    business_name: str
    service_type: str
    identifier: Optional[str]


class ClientSearchRequest(BaseModel):
    query: str


class ClientStageUpdateRequest(BaseModel):
    stage: ClientStage


class OfferStatusUpdateRequest(BaseModel):
    status: OfferStatus


class OfferPipelineStageUpdateRequest(BaseModel):
    pipeline_stage: OfferPipelineStageSchema


class ClientBulkUpdateRequest(BaseModel):
    # Accept arbitrary JSON values here; we'll coerce to ints explicitly
    client_ids: List[object]
    owner_email: Optional[str] = None
    stage: Optional[ClientStage] = None



@app.post("/api/get-business-info")
def get_business_info(
    request: BusinessInfoRequest,
    user_info: dict = Depends(verify_google_token),
    db: Session = Depends(get_db),
):
    logging.info(f"Received business info request: {request}")
    result = get_business_information(request.business_name)
    if isinstance(result, dict):
        result["user_email"] = user_info.get("email")
        # Airtable utility data (Contract End Date, Data Requested, Data Recieved) is loaded
        # lazily via GET /api/utility-extra to keep this endpoint fast.
        business_details = result.get("business_details", {}) or {}
        if business_details.get("name"):
            try:
                contact_info = result.get("contact_information", {}) or {}
                gdrive_info = result.get("gdrive", {}) or {}

                business_name = business_details.get("name") or request.business_name
                external_business_id = result.get("record_ID")
                primary_contact_email = contact_info.get("email")
                gdrive_folder_url = gdrive_info.get("folder_url")

                client = upsert_client_from_business_info(
                    db=db,
                    business_name=business_name,
                    external_business_id=external_business_id,
                    primary_contact_email=primary_contact_email,
                    gdrive_folder_url=gdrive_folder_url,
                )
                if client:
                    result["client_id"] = client.id
            except Exception as e:
                logging.error(f"Error upserting Client from business info: {str(e)}")

    logging.info(f"Returning response to frontend: {result}")
    return result


@app.get("/api/utility-extra")
def get_utility_extra(
    business_name: str = Query(..., description="Business name to fetch Airtable utility details for"),
    user_info: dict = Depends(verify_google_token),
):
    """
    Lazy-load Airtable utility data (Contract End Date, Data Requested, Data Recieved)
    and optionally merged linked_utilities/utility_retailers. Does not block get-business-info.
    Returns empty dicts if Airtable is not configured or business not found.
    """
    out = {"linked_utilities": {}, "utility_retailers": {}, "linked_utility_extra": {}}
    logging.info("[utility-extra] request business_name=%r", business_name)
    if not getattr(airtable_client, "USE_AIRTABLE_DIRECT", False) or not airtable_client.AIRTABLE_API_KEY:
        logging.info("[utility-extra] skipped: USE_AIRTABLE_DIRECT or AIRTABLE_API_KEY not set")
        return out
    name = (business_name or "").strip()
    if not name:
        return out
    try:
        loa_record = airtable_client.get_loa_record_by_business_name(name)
        if not loa_record:
            logging.info("[utility-extra] no LOA record found for business_name=%r", name)
            return out
        logging.info("[utility-extra] LOA record id=%s", (loa_record.get("id") or "")[:12])
        linked_utilities, utility_retailers, linked_utility_extra = airtable_client.get_linked_utility_records(loa_record)
        out["linked_utilities"] = linked_utilities
        out["utility_retailers"] = utility_retailers
        out["linked_utility_extra"] = linked_utility_extra
        # Log response shape so you can see what the frontend receives
        logging.info(
            "[utility-extra] response: linked_utilities keys=%s, linked_utility_extra keys=%s",
            list(out["linked_utilities"].keys()),
            list(out["linked_utility_extra"].keys()),
        )
        for uk, extra_list in out["linked_utility_extra"].items():
            logging.info("[utility-extra] linked_utility_extra[%r] count=%s, first=%s", uk, len(extra_list or []), (extra_list or [])[:1])
    except Exception as e:
        logging.warning("Airtable utility-extra failed: %s", e)
    return out


class ElectricityInvoiceRequest(BaseModel):
    business_name: Optional[str] = None
    nmi: Optional[str] = None

class GasInvoiceRequest(BaseModel):
    business_name: Optional[str] = None
    mrin: Optional[str] = None

class WasteInvoiceRequest(BaseModel):
    business_name: Optional[str] = None
    account_number: Optional[str] = None

class OilInvoiceRequest(BaseModel):
    business_name: Optional[str] = None

class CleaningInvoiceRequest(BaseModel):
    business_name: Optional[str] = None

class DataRequest(BaseModel):
    business_name: str
    supplier_name: str
    request_type: str
    details: Optional[str] = None

class RobotDataRequest(BaseModel):
    robot_number: str

class UtilityRecordUpdateRequest(BaseModel):
    """Update Data Requested, Data Recieved (checkbox), or Contract End Date on an Airtable utility record."""
    business_name: str
    utility_type: str  # e.g. "C&I Electricity", "SME Gas", "Waste"
    identifier: str    # NMI, MRIN, account number, etc.
    data_requested: Optional[str] = None   # YYYY-MM-DD
    data_recieved: Optional[Union[str, bool]] = None   # Checkbox in Airtable: send True/False
    contract_end_date: Optional[str] = None  # YYYY-MM-DD
# Add these Pydantic models with your other BaseModel classes
class BusinessSolution(BaseModel):
    id: str
    name: str
    description: str
    category: str
    slides: list[str]
    icon: str

class BusinessInfo(BaseModel):
    businessName: str
    industry: str
    targetMarket: str
    objectives: str

class StrategyPresentationRequest(BaseModel):
    businessInfo: dict
    selectedStrategies: list[str]
    coverPageTemplateId: str
    strategyTemplates: list[dict]
    placeholders: dict
   

@app.post("/api/get-electricity-ci-info")
def get_electricity_ci_info(
    request: ElectricityInvoiceRequest,
    user_info: dict = Depends(verify_google_token)
):
    if not request.business_name and not request.nmi:
        raise HTTPException(status_code=400, detail="Either business_name or nmi is required")
    
    logging.info(f"Received C&I electricity info request: business_name={request.business_name}, nmi={request.nmi}")
    data = get_electricity_ci_latest_invoice_information(
        business_name=request.business_name,
        nmi=request.nmi
    )
    data["user_email"] = user_info.get("email")
    logging.info(f"Returning C&I electricity info to frontend: {data}")
    return data

@app.post("/api/get-electricity-sme-info")
def get_electricity_sme_info(
    request: ElectricityInvoiceRequest,
    user_info: dict = Depends(verify_google_token)
):
    if not request.business_name and not request.nmi:
        raise HTTPException(status_code=400, detail="business_name and nmi are required")
    
    logging.info(f"Received SME electricity info request: business_name={request.business_name}, nmi={request.nmi}")
    data = get_electricity_sme_latest_invoice_information(
        business_name=request.business_name,
        nmi=request.nmi
    )
    data["user_email"] = user_info.get("email")
    logging.info(f"Returning SME electricity info to frontend: {data}")
    return data

@app.post("/api/get-gas-ci-info")
def get_gas_ci_info(
    request: GasInvoiceRequest,
    user_info: dict = Depends(verify_google_token)
):
    if not request.business_name and not request.mrin:
        raise HTTPException(status_code=400, detail="business_name and mrin are required")

    logging.info(f"Received C&I gas info request: business_name={request.business_name}, mrin={request.mrin}")
    data = get_gas_latest_invoice_information(
        business_name=request.business_name,
        mrin=request.mrin
    )
    data["user_email"] = user_info.get("email")
    logging.info(f"Returning C&I gas info to frontend: {data}")
    return data

@app.post("/api/get-gas-sme-info")
def get_gas_sme_info(
    request: GasInvoiceRequest,
    user_info: dict = Depends(verify_google_token)
):
    if not request.business_name and not request.mrin:
        raise HTTPException(status_code=400, detail="business_name and mrin are required")

    logging.info(f"Received SME gas info request: business_name={request.business_name}, mrin={request.mrin}")
    data = get_gas_sme_latest_invoice_information(
        business_name=request.business_name,
        mrin=request.mrin
    )
    data["user_email"] = user_info.get("email")
    logging.info(f"Returning SME gas info to frontend: {data}")
    return data

@app.post("/api/get-waste-info")
def get_waste_info(
    request: WasteInvoiceRequest,
    user_info: dict = Depends(verify_google_token)
):
    if not request.business_name and not request.account_number:
        raise HTTPException(status_code=400, detail="business_name and account_number are required")

    logging.info(f"Received waste info request: business_name={request.business_name}, account_number={request.account_number}")
    data = get_waste_latest_invoice_information(
        business_name=request.business_name,
        customer_number=request.account_number
    )
    data["user_email"] = user_info.get("email")
    logging.info(f"Returning waste info to frontend: {data}")
    return data

@app.post("/api/get-oil-info")
def get_oil_info(
    request: OilInvoiceRequest,
    user_info: dict = Depends(verify_google_token)
):
    if not request.business_name:
        raise HTTPException(status_code=400, detail="business_name is required")

    logging.info(f"Received oil info request: business_name={request.business_name}")
    data = get_oil_invoice_information(
        account_name=request.business_name
    )
    data["user_email"] = user_info.get("email")
    logging.info(f"Returning oil info to frontend: {data}")
    return data

@app.post("/api/get-cleaning-info")
def get_cleaning_info(
    request: CleaningInvoiceRequest,
    user_info: dict = Depends(verify_google_token)
):
    if not request.business_name:
        raise HTTPException(status_code=400, detail="business_name is required")

    logging.info(f"Received cleaning info request: business_name={request.business_name}")
    data = get_cleaning_invoice_information(
        account_name=request.business_name
    )
    data["user_email"] = user_info.get("email")
    logging.info(f"Returning cleaning info to frontend: {data}")
    return data

@app.post("/api/get-robot-data")
def get_robot_data(
    request: RobotDataRequest,
    user_info: dict = Depends(verify_google_token)
):
    logging.info(f"Received robot data request: robot_number={request.robot_number}")
    
    try:
        webhook_url = "https://membersaces.app.n8n.cloud/webhook/pudu_robot_data"
        response = httpx.post(webhook_url, json={"robot_number": request.robot_number}, timeout=10)
        
        if response.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Robot data service error: {response.status_code}")
        
        try:
            data = response.json()
        except Exception as e:
            logging.error(f"Failed to parse response: {str(e)}")
            raise HTTPException(status_code=500, detail="Invalid JSON from robot data service")

        if isinstance(data, list) and data:
            data = data[0]
        elif not isinstance(data, dict):
            raise HTTPException(status_code=404, detail="No robot data found")
        
        # Add user email to response
        data["user_email"] = user_info.get("email")
        logging.info(f"Returning robot data to frontend: {data}")
        return data

    except Exception as e:
        logging.error(f"Robot data fetch failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving robot data: {str(e)}")

@app.post("/api/get-utility-information")
def get_utility_information(
    request: UtilityInfoRequest,
    user_info: dict = Depends(verify_google_token)
):
    logging.info(f"Received utility info request: {request}")

    service_type = request.service_type.lower()

    if service_type == "electricity_ci":
        data = get_electricity_ci_latest_invoice_information(
            business_name=request.business_name,
            nmi=request.identifier
        )
    elif service_type == "electricity_sme":
        data = get_electricity_sme_latest_invoice_information(
            business_name=request.business_name,
            nmi=request.identifier
        )
    elif service_type == "gas_ci":
        data = get_gas_latest_invoice_information(
            business_name=request.business_name,
            mrin=request.identifier
        )
    elif service_type == "gas_sme":
        data = get_gas_sme_latest_invoice_information(
            business_name=request.business_name,
            mrin=request.identifier
        )
    elif service_type == "waste":
        data = get_waste_latest_invoice_information(
            business_name=request.business_name,
            customer_number=request.identifier
        )
    elif service_type == "oil":
        data = get_oil_invoice_information(
            account_name=request.business_name
        )
    else:
        raise HTTPException(status_code=400, detail=f"Unknown service_type: {service_type}")

    # Always add user email
    data["user_email"] = user_info.get("email")

    logging.info(f"Returning utility info for {service_type}: {data}")
    return data

@app.post("/api/drive-filing")
def drive_filing_endpoint(
    business_name: str = Form(...),
    gdrive_url: str = Form(...),
    filing_type: str = Form(...),
    contract_status: str = Form(None),
    file: UploadFile = File(...),
    user_info: dict = Depends(verify_google_token)
):
    logging.info(f"Received drive filing request: business_name={business_name}, gdrive_url={gdrive_url}, filing_type={filing_type}, filename={file.filename}")
    file_bytes = file.file.read()
    result = drive_filing(
        file_bytes=file_bytes,
        filename=file.filename,
        business_name=business_name,
        gdrive_url=gdrive_url,
        filing_type=filing_type,
        contract_status=contract_status
    )
    result["user_email"] = user_info.get("email")
    logging.info(f"Returning drive filing response to frontend: {result}")
    return result

@app.post("/api/data-request")
def data_request(
    request: DataRequest,
    user_info: dict = Depends(verify_google_token),
    db: Session = Depends(get_db),
):
    logging.info(f"Received data request: {request}")
    service_type = request.request_type
    account_identifier = (request.details or "").strip()

    # Map service_type to identifier_type and utility context
    if service_type in ["electricity_ci", "electricity_sme"]:
        identifier_type = "NMI"
        utility_type = "electricity"
        utility_type_identifier = (
            "C&I Electricity" if service_type == "electricity_ci" else "SME Electricity"
        )
    elif service_type in ["gas_ci", "gas_sme"]:
        identifier_type = "MRIN"
        utility_type = "gas"
        utility_type_identifier = "C&I Gas" if service_type == "gas_ci" else "SME Gas"
    elif service_type == "waste":
        identifier_type = "account_number"
        utility_type = "waste"
        utility_type_identifier = "Waste"
    else:
        # Fallback to electricity/NMI if an unknown type sneaks through
        identifier_type = "NMI"
        utility_type = "electricity"
        utility_type_identifier = "Electricity"

    raw_result = supplier_data_request(
        supplier_name=request.supplier_name,
        business_name=request.business_name,
        service_type=service_type,
        account_identifier=account_identifier,
        identifier_type=identifier_type,
    )

    # Determine success based on the human-readable message
    message = str(raw_result or "").strip()
    is_success = message.startswith("✅") or "Data request successfully sent" in message

    # On success, upsert client, ensure offer, and record an offer activity
    if is_success and request.business_name:
        try:
            user_email = user_info.get("email")

            client = upsert_client_from_business_info(
                db=db,
                business_name=request.business_name,
                external_business_id=None,
                primary_contact_email=None,
                gdrive_folder_url=None,
            )

            offer = None
            if client:
                offer = get_or_create_offer_for_activity(
                    db=db,
                    client_id=client.id,
                    business_name=request.business_name,
                    utility_type=utility_type,
                    utility_type_identifier=utility_type_identifier,
                    identifier=account_identifier or None,
                    created_by=user_email,
                )

            if offer:
                metadata = {
                    "service_type": service_type,
                    "utility_type": utility_type,
                    "utility_type_identifier": utility_type_identifier,
                    "identifier_type": identifier_type,
                    "identifier": account_identifier or None,
                    "supplier_name": request.supplier_name,
                    "source": "data_request_page",
                }
                try:
                    create_offer_activity(
                        db,
                        offer=offer,
                        client=client,
                        activity_type=OfferActivityType.DATA_REQUEST,
                        metadata=metadata,
                        created_by=user_email,
                    )
                except Exception as act_e:
                    logging.warning(
                        "Failed to create DATA_REQUEST offer activity: %s", act_e
                    )

            # Update Airtable "Data Requested" (and clear "Data Received" for C&I E/G) when direct link is enabled
            try:
                from services.airtable_client import (
                    USE_AIRTABLE_DIRECT,
                    update_utility_record_data_requested,
                )
                if USE_AIRTABLE_DIRECT and account_identifier:
                    from datetime import date
                    today = date.today().isoformat()
                    # For C&I Electricity and C&I Gas, also deselect Data Received (request just sent)
                    set_received_false = utility_type_identifier in ("C&I Electricity", "C&I Gas")
                    if update_utility_record_data_requested(
                        utility_type_identifier, account_identifier, today,
                        data_recieved=False if set_received_false else None,
                    ):
                        logging.info(
                            "Updated Airtable Data Requested for %s %s%s",
                            utility_type_identifier,
                            account_identifier,
                            " (Data Received unchecked)" if set_received_false else "",
                        )
            except Exception as air_e:
                logging.warning("Airtable Data Requested update failed: %s", air_e)
        except Exception as crm_e:
            logging.error("Failed to record CRM data for data request: %s", crm_e)

    response_payload = {
        "status": "success" if is_success else "error",
        "message": message,
        "user_email": user_info.get("email"),
    }
    logging.info(f"Returning data request response to frontend: {response_payload}")
    return response_payload


@app.patch("/api/utility-record")
def update_utility_record_endpoint(
    request: UtilityRecordUpdateRequest,
    user_info: dict = Depends(verify_google_token),
):
    """Update Data Requested, Data Recieved (checkbox), or Contract End Date on a member's utility record in Airtable."""
    logging.info(
        "[utility-record PATCH] request: business_name=%r, utility_type=%r, identifier=%r, data_requested=%r, data_recieved=%r, contract_end_date=%r",
        request.business_name, request.utility_type, request.identifier,
        request.data_requested, request.data_recieved, request.contract_end_date,
    )
    if not airtable_client.AIRTABLE_API_KEY:
        raise HTTPException(status_code=503, detail="Airtable integration is not configured")
    if not getattr(airtable_client, "USE_AIRTABLE_DIRECT", False):
        raise HTTPException(status_code=503, detail="Airtable direct mode is not enabled")
    try:
        ok = airtable_client.update_utility_record(
            request.utility_type,
            request.identifier,
            data_requested=request.data_requested,
            data_recieved=request.data_recieved,
            contract_end_date=request.contract_end_date,
        )
    except Exception as e:
        logging.exception("[utility-record PATCH] Airtable update raised: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
    if not ok:
        logging.warning("[utility-record PATCH] update_utility_record returned False (record not found or Airtable error)")
        raise HTTPException(
            status_code=404,
            detail="Utility record not found or update failed. Check business name, utility type, and identifier. See server logs for Airtable response.",
        )
    return {"status": "success", "message": "Utility record updated"}


@app.get("/api/resources/contract-ending")
def get_contract_ending(
    sync: bool = Query(False, description="If true, run sheet->Airtable sync for missing end dates before returning"),
    month: Optional[int] = Query(None, ge=1, le=12, description="Filter contracts ending in this month"),
    year: Optional[int] = Query(None, ge=2000, le=2100, description="Filter contracts ending in this year"),
    utility_type: Optional[str] = Query(None, description="Filter by utility type: C&I Electricity or C&I Gas"),
    user_info: dict = Depends(verify_google_token),
):
    """
    Return C&I Electricity and C&I Gas utility records split into contracts with end date
    and end dates undefined. Optionally run sync from Google Sheet to Airtable first.
    """
    # Always visible in terminal (uvicorn stdout)
    print(f"[contract-ending] GET sync={sync} month={month} year={year} utility_type={utility_type or 'all'}", flush=True)
    logging.info("[contract-ending] GET sync=%s", sync)

    if not airtable_client.AIRTABLE_API_KEY:
        raise HTTPException(status_code=503, detail="Airtable integration is not configured")
    sync_result = {}
    if sync:
        try:
            print("[contract-ending] Running sync: sheet -> Airtable (missing end dates only)", flush=True)
            logging.info("[contract-ending] Sync requested: pulling contract end dates from Member ACES Data sheet -> Airtable")
            sync_result = sync_contract_end_dates_to_airtable()
            u_e, u_g = sync_result.get("updated_electricity", 0), sync_result.get("updated_gas", 0)
            errs = sync_result.get("errors", [])
            updates = sync_result.get("updates", [])
            print(f"[contract-ending] Sync done: C&I E={u_e} updated, C&I G={u_g} updated, errors={len(errs)}", flush=True)
            for u in updates:
                print(f"  -> {u.get('utility_type')} {u.get('identifier_label')} {u.get('identifier')} -> {u.get('contract_end_date')} (from {u.get('source_sheet')})", flush=True)
            if errs:
                for e in errs:
                    print(f"  ERROR: {e}", flush=True)
            logging.info(
                "[contract-ending] Sync complete: C&I Electricity=%s updated, C&I Gas=%s updated, errors=%s. Updates: %s",
                u_e, u_g, len(errs),
                [f"{u.get('utility_type')} {u.get('identifier_label', '')} {u.get('identifier')} -> {u.get('contract_end_date')}" for u in updates],
            )
        except Exception as e:
            print(f"[contract-ending] Sync FAILED: {e}", flush=True)
            logging.exception("[contract-ending] Sync failed: %s", e)
            sync_result = {"errors": [str(e)], "updates": [], "updated_electricity": 0, "updated_gas": 0}
    contracts_with_end_date: List[dict] = []
    end_dates_undefined: List[dict] = []
    for ut in ("C&I Electricity", "C&I Gas"):
        if utility_type and ut != utility_type:
            continue
        try:
            print(f"[contract-ending] Fetching Airtable records: {ut}", flush=True)
            records = airtable_client.list_all_utility_records(ut)
            n_with_date = sum(1 for r in records if r.get("contract_end_date"))
            n_undefined = len(records) - n_with_date
            print(f"[contract-ending] {ut}: {len(records)} total, {n_with_date} with end date, {n_undefined} undefined", flush=True)
            logging.info("[contract-ending] %s: %s records (%s with end date, %s undefined)", ut, len(records), n_with_date, n_undefined)
            for rec in records:
                item = {
                    "identifier": rec.get("identifier", ""),
                    "utility_type": ut,
                    "contract_end_date": rec.get("contract_end_date"),
                    "retailer": rec.get("retailer", ""),
                    "record_id": rec.get("record_id", ""),
                }
                if rec.get("contract_end_date"):
                    # Optional server-side filter by month/year
                    if month is not None or year is not None:
                        try:
                            parts = rec["contract_end_date"].split("-")
                            if len(parts) >= 2:
                                y, m = int(parts[0]), int(parts[1])
                                if month is not None and m != month:
                                    continue
                                if year is not None and y != year:
                                    continue
                        except (ValueError, IndexError):
                            pass
                    contracts_with_end_date.append(item)
                else:
                    end_dates_undefined.append(item)
        except Exception as e:
            logging.warning("[contract-ending] list_all_utility_records %s failed: %s", ut, e)
            print(f"[contract-ending] Airtable failed for {ut}: {e}", flush=True)
    print(f"[contract-ending] Response: {len(contracts_with_end_date)} with end date, {len(end_dates_undefined)} undefined", flush=True)
    return {
        "contracts_with_end_date": contracts_with_end_date,
        "end_dates_undefined": end_dates_undefined,
        "sync": sync_result if sync else None,
    }


class ContractEndingUpdateRequest(BaseModel):
    """Update contract end date for a utility record (by identifier, no business_name required)."""
    utility_type: str  # "C&I Electricity" or "C&I Gas"
    identifier: str
    contract_end_date: str  # YYYY-MM-DD


@app.patch("/api/resources/contract-ending/update")
def update_contract_ending_record(
    request: ContractEndingUpdateRequest,
    user_info: dict = Depends(verify_google_token),
):
    """Update Contract End Date in Airtable for a single utility record (identifier + utility_type)."""
    if not airtable_client.AIRTABLE_API_KEY:
        raise HTTPException(status_code=503, detail="Airtable integration is not configured")
    if request.utility_type not in ("C&I Electricity", "C&I Gas"):
        raise HTTPException(status_code=400, detail="utility_type must be C&I Electricity or C&I Gas")
    date_str = (request.contract_end_date or "").strip()
    if len(date_str) < 10:
        raise HTTPException(status_code=400, detail="contract_end_date must be YYYY-MM-DD")
    date_str = date_str[:10]
    try:
        ok = airtable_client.update_utility_record(
            request.utility_type,
            request.identifier.strip(),
            contract_end_date=date_str,
        )
    except Exception as e:
        logging.exception("[contract-ending update] %s", e)
        raise HTTPException(status_code=500, detail=str(e))
    if not ok:
        raise HTTPException(status_code=404, detail="Record not found or update failed")
    return {"status": "success", "message": "Contract end date updated"}


class SignedAgreementRequest(BaseModel):
    business_name: str
    contract_type: str
    agreement_type: str = "contract"

from fastapi import Request

# Define the updated request model
class QuoteRequestData(BaseModel):
    business_name: str
    trading_as: Optional[str] = None
    abn: Optional[str] = None
    site_address: Optional[str] = None
    client_name: Optional[str] = None
    client_number: Optional[str] = None
    client_email: Optional[str] = None
    nmi: Optional[str] = None
    mrin: Optional[str] = None
    utility_type: str
    quote_type: str
    commission: str
    start_date: str
    offer_due: str
    yearly_peak_est: int
    yearly_shoulder_est: int
    yearly_off_peak_est: int
    yearly_consumption_est: int
    current_retailer: Optional[str] = None
    loa_file_id: Optional[str] = None
    invoice_file_id: Optional[str] = None
    interval_data_file_id: Optional[str] = None
    user_email: Optional[str] = None
    timestamp: Optional[str] = None

@app.post("/api/send-quote-request")
async def send_quote_request_endpoint(
    request: Request,
    authorization: str = Header(...),
    user_info: dict = None,
    db: Session = Depends(get_db),
):
    # Get the request body
    request_data = await request.json()
    
    # Check if it's an API key or Google token
    if authorization.startswith("Bearer "):
        token = authorization.split("Bearer ")[1]
        
        # Check if it's a simple API key (for Next.js API routes)
        if token == os.getenv("BACKEND_API_KEY", "test-key"):
            # Use session email from request_data if available
            user_info = {"email": request_data.get("user_email", "api_user@example.com")}
        else:
            # Try to verify as Google token
            try:
                user_info = verify_google_token(authorization)
            except Exception as e:
                raise HTTPException(status_code=401, detail="Invalid Google token")
    else:
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    
    try:
        # Extract data from request_data (not request)
        selected_retailers = request_data.get("selected_retailers", [])
        business_name = request_data.get("business_name")
        trading_as = request_data.get("trading_as", "")
        abn = request_data.get("abn", "")
        site_address = request_data.get("site_address", "")
        client_name = request_data.get("client_name", "")
        client_number = request_data.get("client_number", "")
        client_email = request_data.get("client_email", "")
        nmi = request_data.get("nmi")
        mrin = request_data.get("mrin")
        utility_type = request_data.get("utility_type", "")
        quote_type = request_data.get("quote_type", "")
        commission = request_data.get("commission", "0")
        start_date = request_data.get("start_date", "")
        offer_due = request_data.get("offer_due", "")
        yearly_peak_est = request_data.get("yearly_peak_est", 0)
        yearly_shoulder_est = request_data.get("yearly_shoulder_est", 0)
        yearly_off_peak_est = request_data.get("yearly_off_peak_est", 0)
        yearly_consumption_est = request_data.get("yearly_consumption_est", 0)
        current_retailer = request_data.get("current_retailer", "")
        loa_file_id = request_data.get("loa_file_id")
        invoice_file_id = request_data.get("invoice_file_id")
        interval_data_file_id = request_data.get("interval_data_file_id")
        user_email = user_info.get("email")
        
        # Validate required fields
        if not business_name:
            raise HTTPException(status_code=400, detail="business_name is required")
        
        if not nmi and not mrin:
            raise HTTPException(status_code=400, detail="Either nmi or mrin is required")
        
        if not selected_retailers:
            raise HTTPException(status_code=400, detail="At least one retailer must be selected")
        
        # Send quote request
        result = send_supplier_quote_request(
            selected_retailers=selected_retailers,
            business_name=business_name,
            trading_as=trading_as,
            abn=abn,
            site_address=site_address,
            client_name=client_name,
            client_number=client_number,
            client_email=client_email,
            nmi=nmi,
            mrin=mrin,
            utility_type=utility_type,
            utility_type_identifier=request_data.get("utility_type_identifier", ""),
            retailer_type_identifier=request_data.get("retailer_type_identifier", ""),
            quote_type=quote_type,
            quote_details=request_data.get("quote_details", ""),
            commission=commission,
            start_date=start_date,
            offer_due=offer_due,
            yearly_peak_est=yearly_peak_est,
            yearly_shoulder_est=yearly_shoulder_est,
            yearly_off_peak_est=yearly_off_peak_est,
            yearly_consumption_est=yearly_consumption_est,
            current_retailer=current_retailer,
            loa_file_id=loa_file_id,
            invoice_file_id=invoice_file_id,
            interval_data_file_id=interval_data_file_id,
            user_email=user_email
        )
        
        logging.info(f"Quote request completed successfully for {business_name}")

        # Try to record an Offer in the CRM database
        try:
            identifier = nmi or mrin or ""
            client = None
            if business_name:
                client = (
                    db.query(Client)
                    .filter(Client.business_name == business_name)
                    .first()
                )

            db_offer = Offer(
                client_id=client.id if client else None,
                business_name=business_name,
                utility_type=utility_type,
                utility_type_identifier=request_data.get("utility_type_identifier", ""),
                identifier=identifier,
                status="requested",
                estimated_value=yearly_consumption_est or None,
                created_by=user_email,
            )
            db.add(db_offer)
            db.commit()
            db.refresh(db_offer)
            # Record offer activity so it shows on the offer detail page
            try:
                create_offer_activity(
                    db,
                    offer=db_offer,
                    client=client,
                    activity_type=OfferActivityType.QUOTE_REQUEST,
                    document_link=result.get("document_link") if isinstance(result, dict) else None,
                    external_id=result.get("quote_request_id") if isinstance(result, dict) else None,
                    metadata={
                        "utility_type": utility_type or None,
                        "utility_type_identifier": request_data.get("utility_type_identifier", "") or None,
                        "nmi": nmi or None,
                        "mrin": mrin or None,
                        "source": "quote_request_page",
                    },
                    created_by=user_email,
                )
            except Exception as act_e:
                logging.warning(f"Failed to create quote_request activity for offer: {act_e}")
        except Exception as e:
            logging.error(f"Failed to create Offer record for quote request: {e}")

        return result
        
    except Exception as e:
        logging.error(f"Quote request failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/signed-agreement-lodgement")
async def signed_agreement_lodgement(
    request: Request,
    user_info: dict = Depends(verify_google_token)
):
    """
    Handle signed agreement lodgement with dynamic file upload support
    """
    # Parse the multipart form data
    form_data = await request.form()
    
    # Extract basic fields
    business_name = form_data.get("business_name")
    contract_type = form_data.get("contract_type")
    agreement_type = form_data.get("agreement_type", "contract")
    file_count = int(form_data.get("file_count", 1))
    
    if not business_name or not contract_type:
        raise HTTPException(status_code=400, detail="business_name and contract_type are required")
    
    logging.info(f"Received signed agreement request: business_name={business_name}, contract_type={contract_type}, agreement_type={agreement_type}, file_count={file_count}")
    
    # Collect all uploaded files
    uploaded_files = []
    filenames = []
    
    for key, value in form_data.items():
        if key.startswith("file_") and hasattr(value, 'filename'):
            # Validate file type
            if not value.filename.lower().endswith('.pdf'):
                raise HTTPException(status_code=400, detail=f"File ({value.filename}) is not a PDF. Only PDF files are accepted")
            uploaded_files.append(value)
            filenames.append(value.filename)
    
    if not uploaded_files:
        raise HTTPException(status_code=400, detail="No valid files provided")
    
    temp_file_paths = []
    try:
        # Process each file
        for file in uploaded_files:
            # Create a temporary file for each upload
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
                content = await file.read()
                temp_file.write(content)
                temp_file_paths.append(temp_file.name)
        
        # Handle single file vs multiple files
        if agreement_type == "contract_multiple_attachments":
            # For multiple attachments, call the new function
            result = send_supplier_signed_agreement_multiple(
                file_paths=temp_file_paths,
                business_name=business_name,
                contract_type=contract_type,
                agreement_type=agreement_type,
                filenames=filenames
            )
        else:
            # For single file, use existing function
            result = send_supplier_signed_agreement(
                file_path=temp_file_paths[0],
                business_name=business_name,
                contract_type=contract_type,
                agreement_type=agreement_type
            )
        
        # Structure the response for the frontend
        response = {
            "status": "success" if "✅" in result else "error",
            "message": result,
            "user_email": user_info.get("email"),
            "contract_type": contract_type,
            "business_name": business_name,
            "agreement_type": agreement_type,
            "file_count": len(uploaded_files),
            "filenames": filenames
        }
        
        logging.info(f"Returning signed agreement response to frontend: {response}")
        return response
        
    except Exception as e:
        logging.error(f"Error processing signed agreement: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing agreement: {str(e)}")
    
    finally:
        # Clean up all temporary files
        for temp_path in temp_file_paths:
            try:
                os.unlink(temp_path)
            except Exception as e:
                logging.warning(f"Could not delete temporary file {temp_path}: {str(e)}")

# Also add an endpoint to get available contract types
@app.get("/api/contract-types")
def get_contract_types(user_info: dict = Depends(verify_google_token)):
    """
    Get available contract types for the frontend dropdown
    """
    from tools.send_supplier_signed_agreement import CONTRACT_EMAIL_MAPPINGS, EOI_EMAIL_MAPPINGS
    
    contracts = list(CONTRACT_EMAIL_MAPPINGS.keys())
    eois = list(EOI_EMAIL_MAPPINGS.keys())
    
    return {
        "contracts": contracts,
        "eois": eois,
        "user_email": user_info.get("email")
    }


@app.get("/api/base1-landing-responses")
def get_base1_landing_responses_endpoint(user_info: dict = Depends(verify_google_token)):
    """
    Get Base 1 Landing Page Responses from Google Sheet (Landing Page Responses tab).
    Returns rows without the Email HTML column. Requires Google auth.
    """
    try:
        rows = get_base1_landing_responses()
        return {"rows": rows, "user_email": user_info.get("email")}
    except Exception as e:
        logging.error(f"Error fetching Base 1 landing responses: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to load Base 1 landing responses")


@app.get("/api/base1-leads")
def get_base1_leads_endpoint(
    user_info: dict = Depends(verify_google_token),
    db: Session = Depends(get_db),
):
    """
    Lead pipeline view backed by Base 1 runs.

    - Source rows: Base 1 Landing Page Responses sheet (via get_base1_landing_responses)
    - Filters out businesses that already exist as CRM clients (by business_name or primary_contact_email)
    - Groups by Company Name so each business appears once (most recent run wins)
    - Enriches with a simple lead status from ClientStatusNote where note_type='lead_status'
      (latest note per business), defaulting to "New" when none exists.

    Returns:
        {
          "rows": [
            {
              "id": str,  # stable per business (company name)
              "company_name": str,
              "contact_name": str | null,
              "contact_email": str | null,
              "contact_number": str | null,
              "state": str | null,
              "timestamp": str | null,
              "drive_folder_url": str | null,
              "base1_review_url": str | null,
              "utility_types": str | null,
              "status": str,  # e.g. "New", "Contacted", "Qualified", "Not a fit"
            },
            ...
          ],
          "user_email": str | null,
        }
    """
    try:
        # 1) Load raw Base 1 rows from Google Sheet
        raw_rows = get_base1_landing_responses() or []

        # 2) Build lookup sets for existing CRM members to filter out
        existing_clients = db.query(Client.business_name, Client.primary_contact_email).all()
        existing_names = {
            (c.business_name or "").strip().lower()
            for c in existing_clients
            if c.business_name
        }
        existing_emails = {
            (c.primary_contact_email or "").strip().lower()
            for c in existing_clients
            if c.primary_contact_email
        }

        # 3) Group Base 1 rows by Company Name and keep the most recent by Timestamp
        grouped_by_company: dict[str, dict] = {}
        for row in raw_rows:
            company = (row.get("Company Name") or "").strip()
            if not company:
                continue

            email = (row.get("Contact Email") or "").strip()
            # Skip if this company or email is already a CRM member
            if company.lower() in existing_names or (email and email.lower() in existing_emails):
                continue

            key = company.lower()
            current = grouped_by_company.get(key)
            ts = row.get("Timestamp") or ""
            if current is None:
                grouped_by_company[key] = row
            else:
                # Prefer the row with the latest timestamp (lexicographically; sheet timestamps are ISO-like)
                current_ts = current.get("Timestamp") or ""
                if ts > current_ts:
                    grouped_by_company[key] = row

        # Nothing left after filtering
        if not grouped_by_company:
            return {"rows": [], "user_email": user_info.get("email")}

        # 4) Fetch latest lead_status note per business in one query
        company_names = [row.get("Company Name") or "" for row in grouped_by_company.values()]
        # Strip blanks
        company_names = [name for name in (n.strip() for n in company_names) if name]

        status_by_business: dict[str, str] = {}
        if company_names:
            notes_q = (
                db.query(ClientStatusNote)
                .filter(
                    ClientStatusNote.business_name.in_(company_names),
                    ClientStatusNote.note_type == "lead_status",
                )
                .order_by(ClientStatusNote.business_name.asc(), ClientStatusNote.created_at.desc())
            )
            for n in notes_q.all():
                name = (n.business_name or "").strip()
                # First (most recent per business) wins thanks to ordering
                if name and name not in status_by_business:
                    status_by_business[name] = (n.note or "").strip() or "New"

        # 5) Build response rows
        rows = []
        for key, row in grouped_by_company.items():
            company = (row.get("Company Name") or "").strip()
            if not company:
                continue
            status = status_by_business.get(company, "New")

            rows.append(
                {
                    "id": company,  # stable id per business for frontend grouping
                    "company_name": company,
                    "contact_name": (row.get("Contact Name") or "").strip() or None,
                    "contact_email": (row.get("Contact Email") or "").strip() or None,
                    "contact_number": (row.get("Contact Number") or "").strip() or None,
                    "state": (row.get("State") or "").strip() or None,
                    "timestamp": (row.get("Timestamp") or "").strip() or None,
                    "drive_folder_url": (row.get("Google Drive Folder") or "").strip() or None,
                    "base1_review_url": (row.get("Base 1 Review") or "").strip() or None,
                    "utility_types": (row.get("Utility Types") or "").strip() or None,
                    "status": status or "New",
                }
            )

        return {"rows": rows, "user_email": user_info.get("email")}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error building Base 1 leads: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to load Base 1 leads")


@app.post("/api/generate-loa")
def generate_loa_endpoint(
    request: DocumentGenerationRequest,
    user_info: dict = Depends(verify_google_token),
    db: Session = Depends(get_db),
):
    """Generate Letter of Authority document"""
    logging.info(f"Received LOA generation request for: {request.business_name}")
    
    try:
        result = loa_generation(
            business_name=request.business_name,
            abn=request.abn,
            trading_as=request.trading_as,
            postal_address=request.postal_address,
            site_address=request.site_address,
            telephone=request.telephone,
            email=request.email,
            contact_name=request.contact_name,
            position=request.position,
            client_folder_url=request.client_folder_url,
        )
        
        result["user_email"] = user_info.get("email")
        logging.info(f"LOA generation completed for: {request.business_name}")
        # Record CRM activity when generation succeeds
        if isinstance(result, dict) and result.get("status") == "success":
            try:
                client = upsert_client_from_business_info(
                    db,
                    business_name=request.business_name,
                    external_business_id=None,
                    primary_contact_email=request.email or None,
                    gdrive_folder_url=request.client_folder_url or None,
                )
                if client:
                    offer = get_or_create_offer_for_activity(
                        db, client.id, request.business_name, "loa",
                        created_by=user_info.get("email"),
                        utility_type_identifier="Letter of Authority",
                    )
                    create_offer_activity(
                        db,
                        offer=offer,
                        client=client,
                        activity_type=OfferActivityType.LOA,
                        document_link=result.get("document_link"),
                        metadata={"source": "document_generation_page"},
                        created_by=user_info.get("email"),
                    )
            except Exception as act_e:
                logging.warning(f"Failed to create LOA activity: {act_e}")
        return result
        
    except Exception as e:
        logging.error(f"Error generating LOA for {request.business_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error generating LOA: {str(e)}")

@app.post("/api/generate-service-agreement")
def generate_service_agreement_endpoint(
    request: DocumentGenerationRequest,
    user_info: dict = Depends(verify_google_token),
    db: Session = Depends(get_db),
):
    """Generate Service Fee Agreement document"""
    logging.info(f"Received Service Agreement generation request for: {request.business_name}")
    
    try:
        result = service_fee_agreement_generation(
            business_name=request.business_name,
            abn=request.abn,
            trading_as=request.trading_as,
            postal_address=request.postal_address,
            site_address=request.site_address,
            telephone=request.telephone,
            email=request.email,
            contact_name=request.contact_name,
            position=request.position,
            client_folder_url=request.client_folder_url,
        )
        
        result["user_email"] = user_info.get("email")
        logging.info(f"Service Agreement generation completed for: {request.business_name}")
        # Record CRM activity when generation succeeds
        if isinstance(result, dict) and result.get("status") == "success":
            try:
                client = upsert_client_from_business_info(
                    db,
                    business_name=request.business_name,
                    external_business_id=None,
                    primary_contact_email=request.email or None,
                    gdrive_folder_url=request.client_folder_url or None,
                )
                if client:
                    offer = get_or_create_offer_for_activity(
                        db, client.id, request.business_name, "service_agreement",
                        created_by=user_info.get("email"),
                        utility_type_identifier="Service Fee Agreement",
                    )
                    create_offer_activity(
                        db,
                        offer=offer,
                        client=client,
                        activity_type=OfferActivityType.SERVICE_AGREEMENT,
                        document_link=result.get("document_link"),
                        metadata={"source": "document_generation_page"},
                        created_by=user_info.get("email"),
                    )
            except Exception as act_e:
                logging.warning(f"Failed to create service_agreement activity: {act_e}")
        return result
        
    except Exception as e:
        logging.error(f"Error generating Service Agreement for {request.business_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error generating Service Agreement: {str(e)}")

@app.post("/api/generate-eoi")
def generate_eoi_endpoint(
    request: EOIGenerationRequest,
    user_info: dict = Depends(verify_google_token),
    db: Session = Depends(get_db),
):
    """Generate Expression of Interest document"""
    logging.info(f"Received EOI generation request for: {request.business_name}, type: {request.expression_type}")
    
    try:
        result = expression_of_interest_generation(
            business_name=request.business_name,
            abn=request.abn,
            trading_as=request.trading_as,
            postal_address=request.postal_address,
            site_address=request.site_address,
            telephone=request.telephone,
            email=request.email,
            contact_name=request.contact_name,
            position=request.position,
            expression_type=request.expression_type,
            client_folder_url=request.client_folder_url,
        )
        
        result["user_email"] = user_info.get("email")
        logging.info(f"EOI generation completed for: {request.business_name}")
        # Record CRM activity when generation succeeds
        if isinstance(result, dict) and result.get("status") == "success":
            try:
                client = upsert_client_from_business_info(
                    db,
                    business_name=request.business_name,
                    external_business_id=None,
                    primary_contact_email=request.email or None,
                    gdrive_folder_url=request.client_folder_url or None,
                )
                if client:
                    offer = get_or_create_offer_for_activity(
                        db, client.id, request.business_name, "eoi",
                        created_by=user_info.get("email"),
                        utility_type_identifier="Expression of Interest",
                    )
                    create_offer_activity(
                        db,
                        offer=offer,
                        client=client,
                        activity_type=OfferActivityType.EOI,
                        document_link=result.get("document_link"),
                        metadata={
                            "expression_type": request.expression_type,
                            "source": "document_generation_page",
                        },
                        created_by=user_info.get("email"),
                    )
            except Exception as act_e:
                logging.warning(f"Failed to create EOI activity: {act_e}")
        return result
        
    except Exception as e:
        logging.error(f"Error generating EOI for {request.business_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error generating EOI: {str(e)}")

@app.post("/api/generate-engagement-form")
def generate_engagement_form_endpoint(
    request: EngagementFormGenerationRequest,
    user_info: dict = Depends(verify_google_token),
    db: Session = Depends(get_db),
):
    """Generate Engagement Form document"""
    logging.info(f"Received Engagement Form generation request for: {request.business_name}, type: {request.engagement_form_type}")
    
    try:
        result = engagement_form_generation(
            business_name=request.business_name,
            abn=request.abn,
            trading_as=request.trading_as,
            postal_address=request.postal_address,
            site_address=request.site_address,
            telephone=request.telephone,
            email=request.email,
            contact_name=request.contact_name,
            position=request.position,
            engagement_form_type=request.engagement_form_type,
            client_folder_url=request.client_folder_url,
        )
        
        result["user_email"] = user_info.get("email")
        logging.info(f"Engagement Form generation completed for: {request.business_name}")
        # Record offer activity when generation succeeds (additive; does not change response)
        if isinstance(result, dict) and result.get("status") == "success":
            try:
                client = upsert_client_from_business_info(
                    db,
                    business_name=request.business_name,
                    external_business_id=None,
                    primary_contact_email=request.email or None,
                    gdrive_folder_url=request.client_folder_url or None,
                )
                if client:
                    offer = get_or_create_offer_for_activity(
                        db, client.id, request.business_name, "engagement_form",
                        created_by=user_info.get("email"),
                        utility_type_identifier="Engagement form",
                    )
                    create_offer_activity(
                        db,
                        offer=offer,
                        client=client,
                        activity_type=OfferActivityType.ENGAGEMENT_FORM,
                        document_link=result.get("document_link"),
                        metadata={
                            "form_type": request.engagement_form_type,
                            "source": "document_generation_page",
                        },
                        created_by=user_info.get("email"),
                    )
            except Exception as act_e:
                logging.warning(f"Failed to create engagement_form activity: {act_e}")
        return result
        
    except Exception as e:
        logging.error(f"Error generating Engagement Form for {request.business_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error generating Engagement Form: {str(e)}")

@app.post("/api/generate-ghg-offer")
def generate_ghg_offer_endpoint(
    request: DocumentGenerationRequest,
    user_info: dict = Depends(verify_google_token),
    db: Session = Depends(get_db),
):
    """Generate GHG Offer document"""
    logging.info(f"Received GHG Offer generation request for: {request.business_name}")
    
    try:
        result = ghg_offer_generation(
            business_name=request.business_name,
            abn=request.abn,
            trading_as=request.trading_as,
            postal_address=request.postal_address,
            site_address=request.site_address,
            telephone=request.telephone,
            email=request.email,
            contact_name=request.contact_name,
            position=request.position,
            client_folder_url=request.client_folder_url,
        )
        
        result["user_email"] = user_info.get("email")
        logging.info(f"GHG Offer generation completed for: {request.business_name}")
        # Record offer activity when generation succeeds (additive; does not change response)
        if isinstance(result, dict) and result.get("status") == "success":
            try:
                client = upsert_client_from_business_info(
                    db,
                    business_name=request.business_name,
                    external_business_id=None,
                    primary_contact_email=request.email or None,
                    gdrive_folder_url=request.client_folder_url or None,
                )
                if client:
                    offer = get_or_create_offer_for_activity(
                        db, client.id, request.business_name, "ghg",
                        created_by=user_info.get("email"),
                        utility_type_identifier="Electricity",
                    )
                    doc_link = result.get("document_link")
                    create_offer_activity(
                        db,
                        offer=offer,
                        client=client,
                        activity_type=OfferActivityType.GHG_OFFER,
                        document_link=doc_link,
                        metadata={
                            "utility_type": "electricity",
                            "source": "document_generation_page",
                        },
                        created_by=user_info.get("email"),
                    )
            except Exception as act_e:
                logging.warning(f"Failed to create ghg_offer activity: {act_e}")
        return result
        
    except Exception as e:
        logging.error(f"Error generating GHG Offer for {request.business_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error generating GHG Offer: {str(e)}")

def get_google_service(token: str, service_name: str, version: str):
    """Create a Google API service client with the provided token"""
    try:
        # Try to use the token directly without refresh capabilities
        credentials = Credentials(token=token)
        
        service = build(service_name, version, credentials=credentials)
        return service
    except Exception as e:
        logging.error(f"Error creating Google service: {str(e)}")
        raise HTTPException(status_code=401, detail=f"Invalid Google token: {str(e)}")

# Solution-specific slide templates
SOLUTION_SLIDE_TEMPLATES = {
    "energy_efficiency": {
        "Energy Audit Results": {
            "content": "• Current energy consumption analysis\n• Identified inefficiencies and waste areas\n• Benchmarking against industry standards\n• Key findings and opportunities",
            "layout": "TITLE_AND_BODY"
        },
        "Cost Savings Analysis": {
            "content": "• Annual energy cost breakdown\n• Projected savings by improvement area\n• Payback period analysis\n• Long-term financial benefits",
            "layout": "TITLE_AND_BODY"
        },
        "Implementation Timeline": {
            "content": "• Phase 1: Quick wins (0-3 months)\n• Phase 2: Medium-term improvements (3-12 months)\n• Phase 3: Major upgrades (12+ months)\n• Milestone tracking and review points",
            "layout": "TITLE_AND_BODY"
        },
        "ROI Projections": {
            "content": "• Investment requirements by phase\n• Expected annual savings\n• Return on investment timeline\n• Risk mitigation strategies",
            "layout": "TITLE_AND_BODY"
        }
    },
    "waste_management": {
        "Current Waste Analysis": {
            "content": "• Waste stream assessment\n• Current disposal costs\n• Recycling rates and opportunities\n• Compliance status review",
            "layout": "TITLE_AND_BODY"
        },
        "Reduction Strategies": {
            "content": "• Source reduction initiatives\n• Process optimization opportunities\n• Employee engagement programs\n• Vendor collaboration strategies",
            "layout": "TITLE_AND_BODY"
        },
        "Recycling Programs": {
            "content": "• Material recovery opportunities\n• Partnership with recycling vendors\n• Staff training and awareness\n• Monitoring and reporting systems",
            "layout": "TITLE_AND_BODY"
        },
        "Cost Benefits": {
            "content": "• Reduced disposal fees\n• Revenue from recyclable materials\n• Improved operational efficiency\n• Enhanced brand reputation",
            "layout": "TITLE_AND_BODY"
        }
    },
    "renewable_energy": {
        "Renewable Options": {
            "content": "• Solar energy potential assessment\n• Wind energy feasibility\n• Alternative renewable sources\n• Grid integration considerations",
            "layout": "TITLE_AND_BODY"
        },
        "Installation Plan": {
            "content": "• Site preparation requirements\n• Equipment procurement timeline\n• Installation phases and milestones\n• Testing and commissioning schedule",
            "layout": "TITLE_AND_BODY"
        },
        "Energy Independence": {
            "content": "• Reduced grid dependency\n• Energy security benefits\n• Resilience during outages\n• Long-term sustainability goals",
            "layout": "TITLE_AND_BODY"
        },
        "Financial Benefits": {
            "content": "• Capital cost analysis\n• Operating cost savings\n• Government incentives and rebates\n• Financing options available",
            "layout": "TITLE_AND_BODY"
        }
    },
    "carbon_offsetting": {
        "Carbon Footprint Assessment": {
            "content": "• Current emissions baseline\n• Scope 1, 2, and 3 emissions\n• Industry benchmarking\n• Reduction targets and goals",
            "layout": "TITLE_AND_BODY"
        },
        "Offset Strategies": {
            "content": "• Direct emission reduction projects\n• Carbon credit purchasing programs\n• Nature-based solutions\n• Technology investment options",
            "layout": "TITLE_AND_BODY"
        },
        "Certification Process": {
            "content": "• Third-party verification requirements\n• Certification standards (VCS, Gold Standard)\n• Documentation and reporting needs\n• Ongoing monitoring protocols",
            "layout": "TITLE_AND_BODY"
        },
        "Impact Measurement": {
            "content": "• Key performance indicators\n• Tracking and reporting systems\n• Stakeholder communication strategy\n• Continuous improvement processes",
            "layout": "TITLE_AND_BODY"
        }
    },
    "supply_chain": {
        "Current State Analysis": {
            "content": "• Supply chain mapping and visualization\n• Key performance metrics assessment\n• Bottleneck identification\n• Risk and vulnerability analysis",
            "layout": "TITLE_AND_BODY"
        },
        "Optimization Opportunities": {
            "content": "• Process improvement initiatives\n• Technology integration possibilities\n• Vendor consolidation strategies\n• Inventory management enhancements",
            "layout": "TITLE_AND_BODY"
        },
        "Implementation Strategy": {
            "content": "• Phased rollout approach\n• Change management requirements\n• Training and skill development\n• Technology deployment timeline",
            "layout": "TITLE_AND_BODY"
        },
        "Performance Metrics": {
            "content": "• Cost reduction targets\n• Efficiency improvement goals\n• Quality enhancement measures\n• Customer satisfaction indicators",
            "layout": "TITLE_AND_BODY"
        }
    },
    "digital_transformation": {
        "Digital Assessment": {
            "content": "• Current technology landscape\n• Digital maturity evaluation\n• Gap analysis and opportunities\n• Competitive positioning review",
            "layout": "TITLE_AND_BODY"
        },
        "Technology Roadmap": {
            "content": "• Strategic technology priorities\n• Implementation timeline\n• Integration requirements\n• Infrastructure considerations",
            "layout": "TITLE_AND_BODY"
        },
        "Change Management": {
            "content": "• Organizational readiness assessment\n• Training and development programs\n• Communication strategies\n• Success measurement frameworks",
            "layout": "TITLE_AND_BODY"
        },
        "Success Metrics": {
            "content": "• Productivity improvement targets\n• Customer experience enhancements\n• Operational efficiency gains\n• Revenue growth opportunities",
            "layout": "TITLE_AND_BODY"
        }
    },
    "compliance_management": {
        "Compliance Requirements": {
            "content": "• Regulatory landscape overview\n• Applicable laws and standards\n• Industry-specific requirements\n• Upcoming regulatory changes",
            "layout": "TITLE_AND_BODY"
        },
        "Gap Analysis": {
            "content": "• Current compliance status\n• Identified gaps and deficiencies\n• Risk assessment and prioritization\n• Resource requirements for compliance",
            "layout": "TITLE_AND_BODY"
        },
        "Action Plan": {
            "content": "• Remediation strategies\n• Implementation timeline\n• Responsibility assignments\n• Budget and resource allocation",
            "layout": "TITLE_AND_BODY"
        },
        "Monitoring Framework": {
            "content": "• Ongoing compliance tracking\n• Audit and review schedules\n• Reporting mechanisms\n• Continuous improvement processes",
            "layout": "TITLE_AND_BODY"
        }
    },
    "cost_reduction": {
        "Cost Analysis": {
            "content": "• Current cost structure breakdown\n• Spend analysis by category\n• Benchmarking against industry peers\n• Cost driver identification",
            "layout": "TITLE_AND_BODY"
        },
        "Reduction Opportunities": {
            "content": "• Process optimization initiatives\n• Vendor negotiation strategies\n• Technology-driven efficiencies\n• Organizational restructuring options",
            "layout": "TITLE_AND_BODY"
        },
        "Implementation Plan": {
            "content": "• Quick wins and immediate actions\n• Medium-term improvement projects\n• Long-term strategic initiatives\n• Risk mitigation strategies",
            "layout": "TITLE_AND_BODY"
        },
        "Savings Tracking": {
            "content": "• Measurement methodologies\n• Reporting and dashboard systems\n• Performance monitoring protocols\n• Continuous improvement frameworks",
            "layout": "TITLE_AND_BODY"
        }
    },
    "sustainability_reporting": {
        "ESG Framework": {
            "content": "• Environmental performance indicators\n• Social responsibility metrics\n• Governance and ethics standards\n• Stakeholder engagement strategies",
            "layout": "TITLE_AND_BODY"
        },
        "Data Collection": {
            "content": "• Data sources and systems\n• Collection methodologies\n• Quality assurance processes\n• Automation opportunities",
            "layout": "TITLE_AND_BODY"
        },
        "Report Structure": {
            "content": "• Report format and standards\n• Key messaging and narratives\n• Visual presentation strategies\n• Distribution and communication plan",
            "layout": "TITLE_AND_BODY"
        },
        "Stakeholder Communication": {
            "content": "• Stakeholder mapping and analysis\n• Communication channels and frequency\n• Feedback collection mechanisms\n• Engagement improvement strategies",
            "layout": "TITLE_AND_BODY"
        }
    }
}

def generate_personalized_content(slide_title: str, base_content: str, business_info: BusinessInfo, solution_name: str) -> str:
    """Generate personalized slide content based on business information"""
    
    # Create personalized content by incorporating business details
    personalized_content = base_content
    
    # Add business-specific context
    if business_info.businessName:
        if "analysis" in slide_title.lower() or "assessment" in slide_title.lower():
            personalized_content += f"\n\nFor {business_info.businessName}:"
        
    if business_info.industry:
        personalized_content += f"\n• Industry focus: {business_info.industry}"
        
    if business_info.targetMarket:
        personalized_content += f"\n• Target market considerations: {business_info.targetMarket}"
        
    if business_info.objectives:
        personalized_content += f"\n• Alignment with objectives: {business_info.objectives}"
    
    return personalized_content

def create_title_slide_requests(business_info: BusinessInfo, title: str) -> list:
    """Create requests for the title slide"""
    requests = []
    
    # Add title slide
    requests.append({
        'createSlide': {
            'slideLayoutReference': {
                'predefinedLayout': 'TITLE_AND_SUBTITLE'
            },
            'placeholderIdMappings': [
                {
                    'layoutPlaceholder': {
                        'type': 'TITLE',
                        'index': 0
                    },
                    'objectId': 'title_text'
                },
                {
                    'layoutPlaceholder': {
                        'type': 'SUBTITLE',
                        'index': 0
                    },
                    'objectId': 'subtitle_text'
                }
            ]
        }
    })
    
    # Update title text
    requests.append({
        'insertText': {
            'objectId': 'title_text',
            'text': title
        }
    })
    
    # Update subtitle with business info
    subtitle_text = f"Business Solutions Strategy"
    if business_info.businessName:
        subtitle_text += f"\n{business_info.businessName}"
    if business_info.industry:
        subtitle_text += f"\nIndustry: {business_info.industry}"
    
    requests.append({
        'insertText': {
            'objectId': 'subtitle_text',
            'text': subtitle_text
        }
    })
    
    return requests

def create_solution_slides_requests(slides_service, presentation_id: str, solution: BusinessSolution, business_info: BusinessInfo) -> list:
    """Create requests for solution-specific slides"""
    requests = []
    
    if solution.id not in SOLUTION_SLIDE_TEMPLATES:
        return requests
    
    solution_templates = SOLUTION_SLIDE_TEMPLATES[solution.id]
    
    for slide_title, slide_data in solution_templates.items():
        # Create slide
        slide_id = f"slide_{solution.id}_{slide_title.replace(' ', '_').lower()}"
        
        requests.append({
            'createSlide': {
                'slideLayoutReference': {
                    'predefinedLayout': 'TITLE_AND_BODY'
                },
                'placeholderIdMappings': [
                    {
                        'layoutPlaceholder': {
                            'type': 'TITLE',
                            'index': 0
                        },
                        'objectId': f"{slide_id}_title"
                    },
                    {
                        'layoutPlaceholder': {
                            'type': 'BODY',
                            'index': 0
                        },
                        'objectId': f"{slide_id}_body"
                    }
                ]
            }
        })
        
        # Add title
        requests.append({
            'insertText': {
                'objectId': f"{slide_id}_title",
                'text': slide_title
            }
        })
        
        # Add personalized content
        personalized_content = generate_personalized_content(
            slide_title, 
            slide_data['content'], 
            business_info, 
            solution.name
        )
        
        requests.append({
            'insertText': {
                'objectId': f"{slide_id}_body",
                'text': personalized_content
            }
        })
    
    return requests

def generate_presentation_pdf(drive_service, presentation_id: str) -> str:
    """Generate PDF from Google Slides presentation"""
    try:
        # Export as PDF
        pdf_response = drive_service.files().export_media(
            fileId=presentation_id,
            mimeType='application/pdf'
        ).execute()
        
        # Return the presentation ID for PDF generation
        return f"https://docs.google.com/presentation/d/{presentation_id}/export/pdf"
        
    except Exception as e:
        logging.error(f"Error generating PDF: {str(e)}")
        return None

@app.get("/api/eoi-types")
def get_eoi_types_endpoint(user_info: dict = Depends(verify_google_token)):
    """Get available Expression of Interest types"""
    return {
        "eoi_types": get_available_eoi_types(),
        "user_email": user_info.get("email")
    }

@app.get("/api/engagement-form-types")
def get_engagement_form_types_endpoint(user_info: dict = Depends(verify_google_token)):
    """Get available Engagement Form types"""
    return {
        "engagement_form_types": get_available_engagement_form_types(),
        "user_email": user_info.get("email")
    }

@app.post("/api/generate-loa-new")
def generate_loa_new_endpoint(
    request: NewLOAGeneration,
    user_info: dict = Depends(verify_google_token),
    db: Session = Depends(get_db),
):
    """Generate Letter of Authority document for new clients (without client folder URL)"""
    logging.info(f"Received new LOA generation request for: {request.business_name}")
    
    try:
        
        result_message = loa_generation_new(
            business_name=request.business_name,
            abn=request.abn,
            trading_as=request.trading_as,
            postal_address=request.postal_address,
            site_address=request.site_address,
            telephone=request.telephone,
            email=request.email,
            contact_name=request.contact_name,
            position=request.position,
        )
        
        # Parse the message to extract document link
        document_link = None
        if "You can access it here:" in result_message:
            link_start = result_message.find("You can access it here:") + len("You can access it here:")
            link_end = result_message.rfind(".")  # Find the LAST period in the message
            if link_end != -1:
                document_link = result_message[link_start:link_end].strip()
        
        # Create the structured response
        result = {
            "status": "success",
            "message": f'The Letter of Authority for "{request.business_name}" has been successfully generated.',
            "document_link": document_link,
            "user_email": user_info.get("email")
        }
        
        logging.info(f"New LOA generation completed for: {request.business_name}")
        # Record CRM activity when generation succeeds
        if result.get("status") == "success":
            try:
                client = upsert_client_from_business_info(
                    db,
                    business_name=request.business_name,
                    external_business_id=None,
                    primary_contact_email=request.email or None,
                    gdrive_folder_url=None,
                )
                if client:
                    offer = get_or_create_offer_for_activity(
                        db, client.id, request.business_name, "loa",
                        created_by=user_info.get("email"),
                        utility_type_identifier="Letter of Authority",
                    )
                    create_offer_activity(
                        db,
                        offer=offer,
                        client=client,
                        activity_type=OfferActivityType.LOA,
                        document_link=document_link,
                        metadata={"source": "document_generation_page", "variant": "new"},
                        created_by=user_info.get("email"),
                    )
            except Exception as act_e:
                logging.warning(f"Failed to create LOA activity: {act_e}")
        return result
        
    except Exception as e:
        logging.error(f"Error generating new LOA for {request.business_name}: {str(e)}")
        return {
            "status": "error",
            "message": f"Error generating LOA: {str(e)}",
            "document_link": None,
            "user_email": user_info.get("email")
        }

@app.post("/api/generate-service-agreement-new")
def generate_service_agreement_new_endpoint(
    request: NewLOAGeneration,
    user_info: dict = Depends(verify_google_token),
    db: Session = Depends(get_db),
):
    """Generate Service Fee Agreement document for new clients (without client folder URL)"""
    logging.info(f"Received new Service Agreement generation request for: {request.business_name}")
    
    try:
        
        result_message = service_agreement_generation_new(
            business_name=request.business_name,
            abn=request.abn,
            trading_as=request.trading_as,
            postal_address=request.postal_address,
            site_address=request.site_address,
            telephone=request.telephone,
            email=request.email,
            contact_name=request.contact_name,
            position=request.position,
        )
        
        # Parse the message to extract document link
        document_link = None
        if "You can access it here:" in result_message:
            link_start = result_message.find("You can access it here:") + len("You can access it here:")
            link_end = result_message.rfind(".")  # Find the LAST period in the message
            if link_end != -1:
                document_link = result_message[link_start:link_end].strip()
        
        # Create the structured response
        result = {
            "status": "success",
            "message": f'The Service Fee Agreement for "{request.business_name}" has been successfully generated.',
            "document_link": document_link,
            "user_email": user_info.get("email")
        }
        
        logging.info(f"New Service Agreement generation completed for: {request.business_name}")
        # Record CRM activity when generation succeeds
        if result.get("status") == "success":
            try:
                client = upsert_client_from_business_info(
                    db,
                    business_name=request.business_name,
                    external_business_id=None,
                    primary_contact_email=request.email or None,
                    gdrive_folder_url=None,
                )
                if client:
                    offer = get_or_create_offer_for_activity(
                        db, client.id, request.business_name, "service_agreement",
                        created_by=user_info.get("email"),
                        utility_type_identifier="Service Fee Agreement",
                    )
                    create_offer_activity(
                        db,
                        offer=offer,
                        client=client,
                        activity_type=OfferActivityType.SERVICE_AGREEMENT,
                        document_link=document_link,
                        metadata={"source": "document_generation_page", "variant": "new"},
                        created_by=user_info.get("email"),
                    )
            except Exception as act_e:
                logging.warning(f"Failed to create service_agreement activity: {act_e}")
        return result
        
    except Exception as e:
        logging.error(f"Error generating new Service Agreement for {request.business_name}: {str(e)}")
        return {
            "status": "error",
            "message": f"Error generating Service Agreement: {str(e)}",
            "document_link": None,
            "user_email": user_info.get("email")
        }

@app.post("/api/generate-loa-sfa-new")
def generate_loa_sfa_new_endpoint(
    request: NewLOAGeneration,
    user_info: dict = Depends(verify_google_token),
    db: Session = Depends(get_db),
):
    """Generate both LOA and Service Fee Agreement documents for new clients (without client folder URL)"""
    logging.info(f"Received LOA and SFA generation request for: {request.business_name}")
    
    loa_document_link = None
    sfa_document_link = None
    errors = []
    
    # Generate LOA document
    try:
        logging.info(f"Generating LOA for: {request.business_name}")
        loa_result_message = loa_generation_new(
            business_name=request.business_name,
            abn=request.abn,
            trading_as=request.trading_as,
            postal_address=request.postal_address,
            site_address=request.site_address,
            telephone=request.telephone,
            email=request.email,
            contact_name=request.contact_name,
            position=request.position,
        )
        
        # Parse the LOA message to extract document link
        if "You can access it here:" in loa_result_message:
            link_start = loa_result_message.find("You can access it here:") + len("You can access it here:")
            link_end = loa_result_message.rfind(".")  # Find the LAST period in the message
            if link_end != -1:
                loa_document_link = loa_result_message[link_start:link_end].strip()
        
        logging.info(f"LOA generation completed for: {request.business_name}")
    except Exception as e:
        error_msg = f"Error generating LOA: {str(e)}"
        logging.error(f"Error generating LOA for {request.business_name}: {str(e)}")
        errors.append(error_msg)
    
    # Generate SFA document
    try:
        logging.info(f"Generating SFA for: {request.business_name}")
        sfa_result_message = service_agreement_generation_new(
            business_name=request.business_name,
            abn=request.abn,
            trading_as=request.trading_as,
            postal_address=request.postal_address,
            site_address=request.site_address,
            telephone=request.telephone,
            email=request.email,
            contact_name=request.contact_name,
            position=request.position,
        )
        
        # Parse the SFA message to extract document link
        if "You can access it here:" in sfa_result_message:
            link_start = sfa_result_message.find("You can access it here:") + len("You can access it here:")
            link_end = sfa_result_message.rfind(".")  # Find the LAST period in the message
            if link_end != -1:
                sfa_document_link = sfa_result_message[link_start:link_end].strip()
        
        logging.info(f"SFA generation completed for: {request.business_name}")
    except Exception as e:
        error_msg = f"Error generating SFA: {str(e)}"
        logging.error(f"Error generating SFA for {request.business_name}: {str(e)}")
        errors.append(error_msg)
    
    # Determine overall status
    if loa_document_link and sfa_document_link:
        status = "success"
        message = f'Both Letter of Authority and Service Fee Agreement for "{request.business_name}" have been successfully generated.'
    elif loa_document_link or sfa_document_link:
        status = "partial_success"
        message = f'One document generated for "{request.business_name}". ' + " ".join(errors)
    else:
        status = "error"
        message = f'Failed to generate documents for "{request.business_name}". ' + " ".join(errors)
    
    # Create the structured response
    result = {
        "status": status,
        "message": message,
        "loa_document_link": loa_document_link,
        "sfa_document_link": sfa_document_link,
        "user_email": user_info.get("email")
    }
    
    # Record CRM activity for each document generated
    if status in ("success", "partial_success") and (loa_document_link or sfa_document_link):
        try:
            client = upsert_client_from_business_info(
                db,
                business_name=request.business_name,
                external_business_id=None,
                primary_contact_email=request.email or None,
                gdrive_folder_url=None,
            )
            if client:
                if loa_document_link:
                    offer_loa = get_or_create_offer_for_activity(
                        db, client.id, request.business_name, "loa",
                        created_by=user_info.get("email"),
                        utility_type_identifier="Letter of Authority",
                    )
                    create_offer_activity(
                        db,
                        offer=offer_loa,
                        client=client,
                        activity_type=OfferActivityType.LOA,
                        document_link=loa_document_link,
                        metadata={"source": "document_generation_page", "variant": "loa_sfa_new"},
                        created_by=user_info.get("email"),
                    )
                if sfa_document_link:
                    offer_sfa = get_or_create_offer_for_activity(
                        db, client.id, request.business_name, "service_agreement",
                        created_by=user_info.get("email"),
                        utility_type_identifier="Service Fee Agreement",
                    )
                    create_offer_activity(
                        db,
                        offer=offer_sfa,
                        client=client,
                        activity_type=OfferActivityType.SERVICE_AGREEMENT,
                        document_link=sfa_document_link,
                        metadata={"source": "document_generation_page", "variant": "loa_sfa_new"},
                        created_by=user_info.get("email"),
                    )
        except Exception as act_e:
            logging.warning(f"Failed to create LOA/SFA activity: {act_e}")
    
    logging.info(f"LOA and SFA generation completed for: {request.business_name} - LOA: {bool(loa_document_link)}, SFA: {bool(sfa_document_link)}")
    return result

def extract_google_drive_id(url: str) -> str:
    """Extract Google Drive file/folder ID from URL"""
    if not url:
        return None
    
    if '/d/' in url:
        return url.split('/d/')[1].split('/')[0]
    elif 'id=' in url:
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(url)
        return parse_qs(parsed.query).get('id', [None])[0]
    elif '/folders/' in url:
        return url.split('/folders/')[1].split('?')[0]
    
    return None

@app.post("/api/generate-strategy-presentation-real")
def generate_strategy_presentation_real_endpoint(
    request: StrategyPresentationRequest,
    authorization: str = Header(...),
    user_info: dict = Depends(verify_google_token),
    db: Session = Depends(get_db),
):
    try:
        # Your Apps Script Web App URL
        APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbyPEwq6-loam7vlM22i0NCZ2K25bDb_VrnTqdz-WTgGosPaMiUTwrT7YtlSoL4feiqD/exec"
        
        # Extract folder URL from business info
        client_folder_url = request.businessInfo.get('client_folder_url', '') if hasattr(request, 'clientFolderUrl') else request.businessInfo.get('client_folder_url', '')
        
        # Prepare data for Apps Script
        payload = {
            "businessInfo": request.businessInfo,
            "selectedStrategies": request.selectedStrategies,
            "coverPageTemplateId": request.coverPageTemplateId,
            "strategyTemplates": request.strategyTemplates,
            "placeholders": request.placeholders,
            "clientFolderUrl": client_folder_url
        }
        
        # Call Apps Script
        response = requests.post(APPS_SCRIPT_URL, json=payload, timeout=300)  # 5 minute timeout
        
        if response.status_code == 200:
            result = response.json()
            # Record CRM activity when strategy presentation is generated
            business_name = (request.businessInfo.get("businessName") or
                            request.businessInfo.get("business_name") or "").strip()
            if business_name and (result.get("success") is True or result.get("document_link") or result.get("documentLink")):
                try:
                    ext_id = request.businessInfo.get("record_ID")
                    if ext_id is not None:
                        ext_id = str(ext_id)
                    client = upsert_client_from_business_info(
                        db,
                        business_name=business_name,
                        external_business_id=ext_id,
                        primary_contact_email=request.businessInfo.get("email"),
                        gdrive_folder_url=client_folder_url or None,
                    )
                    if client:
                        doc_link = result.get("document_link") or result.get("documentLink")
                        offer = get_or_create_offer_for_activity(
                            db, client.id, business_name, "solution_presentation",
                            created_by=user_info.get("email"),
                            utility_type_identifier="Solution presentation",
                        )
                        create_offer_activity(
                            db,
                            offer=offer,
                            client=client,
                            activity_type=OfferActivityType.SOLUTION_PRESENTATION,
                            document_link=doc_link,
                            metadata={
                                "selected_strategies": request.selectedStrategies,
                                "source": "document_generation_page",
                            },
                            created_by=user_info.get("email"),
                        )
                except Exception as act_e:
                    logging.warning(f"Failed to create solution_presentation activity: {act_e}")
            return result
        else:
            return {
                "success": False,
                "message": f"Apps Script error: {response.status_code}"
            }
            
    except Exception as e:
        logging.error(f"Error calling Apps Script: {str(e)}")
        return {
            "success": False,
            "message": f"Error: {str(e)}"
        }

@app.post("/api/debug-google-token")
def debug_google_token(
    authorization: str = Header(...),
    user_info: dict = Depends(verify_google_token)
):
    """Debug what tokens we have"""
    try:
        user_token = authorization.split("Bearer ")[1]
        
        # Try to inspect the token
        logging.info(f"Token length: {len(user_token)}")
        logging.info(f"Token starts with: {user_token[:50]}...")
        
        # Test if we can create a simple service
        try:
            credentials = Credentials(token=user_token)
            service = build('drive', 'v3', credentials=credentials)
            
            # Try a simple read operation
            files = service.files().list(pageSize=1).execute()
            
            return {
                "success": True,
                "message": "Token works for basic operations",
                "can_read_drive": True
            }
            
        except Exception as e:
            return {
                "success": False,
                "message": f"Token issue: {str(e)}"
            }
            
    except Exception as e:
        return {
            "success": False,
            "message": f"Debug failed: {str(e)}"
        }

# One Month Savings Invoice API Routes
class InvoiceLogRequest(BaseModel):
    invoice_number: str
    business_name: str
    business_abn: Optional[str] = ""
    contact_name: Optional[str] = ""
    contact_email: Optional[str] = ""
    invoice_date: str
    due_date: str
    line_items: List[Dict]
    subtotal: float
    total_gst: float
    total_amount: float
    status: Optional[str] = "Generated"
    created_at: Optional[str] = None

class InvoiceHistoryRequest(BaseModel):
    business_name: str

class NextInvoiceNumberRequest(BaseModel):
    business_name: Optional[str] = None

@app.post("/api/one-month-savings/log")
async def log_invoice_endpoint(
    request: Request,
    authorization: str = Header(...),
    user_info: dict = None,
    db: Session = Depends(get_db),
):
    """Log an invoice to Google Sheets directly or via n8n webhook"""
    logging.info("=== One Month Savings Invoice Log Endpoint Called ===")
    
    # Get the request body
    request_data = await request.json()
    logging.info(f"Request data keys: {list(request_data.keys())}")
    logging.info(f"Invoice number: {request_data.get('invoice_number')}")
    logging.info(f"Business name: {request_data.get('business_name')}")
    logging.info(f"Invoice file ID received: {request_data.get('invoice_file_id')}")
    logging.info(f"Invoice file ID type: {type(request_data.get('invoice_file_id'))}")
    logging.info(f"Invoice file ID empty?: {not request_data.get('invoice_file_id')}")
    
    # Check if it's an API key or Google token
    if authorization.startswith("Bearer "):
        token = authorization.split("Bearer ")[1]
        logging.info(f"Authorization token type: {'API Key' if token == os.getenv('BACKEND_API_KEY', 'test-key') else 'Google Token'}")
        
        # Check if it's a simple API key (for Next.js API routes)
        if token == os.getenv("BACKEND_API_KEY", "test-key"):
            # Use session email from request_data if available
            user_info = {"email": request_data.get("user_email", "api_user@example.com")}
            logging.info(f"Using API key authentication for user: {user_info.get('email')}")
        else:
            # Try to verify as Google token
            try:
                user_info = verify_google_token(authorization)
                logging.info(f"Google token verified for user: {user_info.get('email')}")
            except Exception as e:
                logging.error(f"Token verification failed: {e}")
                raise HTTPException(status_code=401, detail="Invalid Google token")
    else:
        logging.error("Invalid authorization format - missing 'Bearer ' prefix")
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    
    logging.info(f"Processing invoice log request: {request_data.get('invoice_number')} for {request_data.get('business_name')}")
    
    try:
        invoice_file_id = request_data.get("invoice_file_id", "") or request_data.get("file_id", "")
        
        if not invoice_file_id:
            logging.warning(f"⚠️ WARNING: Invoice {request_data.get('invoice_number')} is being logged WITHOUT a file_id!")
            logging.warning(f"⚠️ This means the PDF upload may have failed or the file_id was not returned.")
            logging.warning(f"⚠️ Request data keys: {list(request_data.keys())}")
            logging.warning(f"⚠️ invoice_file_id value: {request_data.get('invoice_file_id')}")
            logging.warning(f"⚠️ file_id value: {request_data.get('file_id')}")
        else:
            logging.info(f"✅ Invoice {request_data.get('invoice_number')} has file_id: {invoice_file_id}")
        
        invoice_data = {
            "invoice_number": request_data.get("invoice_number"),
            "business_name": request_data.get("business_name"),
            "business_abn": request_data.get("business_abn", ""),
            "contact_name": request_data.get("contact_name", ""),
            "contact_email": request_data.get("contact_email", ""),
            "invoice_date": request_data.get("invoice_date"),
            "due_date": request_data.get("due_date"),
            "line_items": request_data.get("line_items", []),
            "subtotal": request_data.get("subtotal", 0),
            "total_gst": request_data.get("total_gst", 0),
            "total_amount": request_data.get("total_amount", 0),
            "status": request_data.get("status", "Generated"),
            "created_at": request_data.get("created_at"),
            "invoice_file_id": invoice_file_id
        }
        
        result = log_invoice_to_sheets(invoice_data)
        result["user_email"] = user_info.get("email")

        # Best-effort: also create a CRM activity so the invoice appears on the member timeline.
        try:
            business_name = invoice_data.get("business_name") or ""
            contact_email = invoice_data.get("contact_email") or None

            if business_name:
                client = upsert_client_from_business_info(
                    db=db,
                    business_name=business_name,
                    external_business_id=None,
                    primary_contact_email=contact_email,
                    gdrive_folder_url=None,
                )

                if client:
                    offer = get_or_create_offer_for_activity(
                        db=db,
                        client_id=client.id,
                        business_name=client.business_name,
                        utility_type="one_month_savings",
                        created_by=user_info.get("email"),
                        utility_type_identifier="1st Month Savings Invoice",
                        identifier=invoice_data.get("invoice_number"),
                    )

                    metadata = {
                        "source": "one_month_savings",
                        "invoice_number": invoice_data.get("invoice_number"),
                        "total_amount": invoice_data.get("total_amount"),
                        "due_date": invoice_data.get("due_date"),
                        "line_items": invoice_data.get("line_items", []),
                        "invoice_file_id": invoice_data.get("invoice_file_id"),
                    }

                    create_offer_activity(
                        db=db,
                        offer=offer,
                        client=client,
                        activity_type=OfferActivityType.ONE_MONTH_SAVINGS_INVOICE,
                        document_link=None,
                        external_id=None,
                        metadata=metadata,
                        created_by=user_info.get("email"),
                    )
        except Exception as e:
            logging.error(
                f"Failed to create CRM activity for 1st Month Savings invoice "
                f"{invoice_data.get('invoice_number')}: {e}"
            )
        
        logging.info(f"Invoice logging completed: {result.get('success')}")
        return result
        
    except Exception as e:
        logging.error(f"Error logging invoice: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error logging invoice: {str(e)}")

@app.post("/api/one-month-savings/history")
async def get_invoice_history_endpoint(
    request: Request,
    authorization: str = Header(...),
    user_info: dict = None
):
    """Get invoice history for a business from Google Sheets directly or via n8n webhook"""
    logging.info("=== One Month Savings Invoice History Endpoint Called ===")
    
    # Get the request body
    request_data = await request.json()
    logging.info(f"Request data: {request_data}")
    
    # Check if it's an API key or Google token
    if authorization.startswith("Bearer "):
        token = authorization.split("Bearer ")[1]
        logging.info(f"Authorization token type: {'API Key' if token == os.getenv('BACKEND_API_KEY', 'test-key') else 'Google Token'}")
        
        # Check if it's a simple API key (for Next.js API routes)
        if token == os.getenv("BACKEND_API_KEY", "test-key"):
            # Use session email from request_data if available
            user_info = {"email": request_data.get("user_email", "api_user@example.com")}
            logging.info(f"Using API key authentication for user: {user_info.get('email')}")
        else:
            # Try to verify as Google token
            try:
                user_info = verify_google_token(authorization)
                logging.info(f"Google token verified for user: {user_info.get('email')}")
            except Exception as e:
                logging.error(f"Token verification failed: {e}")
                raise HTTPException(status_code=401, detail="Invalid Google token")
    else:
        logging.error("Invalid authorization format - missing 'Bearer ' prefix")
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    
    business_name = request_data.get("business_name")
    logging.info(f"Fetching invoice history for business: {business_name}")
    
    try:
        result = get_invoice_history(business_name)
        result["user_email"] = user_info.get("email")
        
        logging.info(f"Invoice history retrieved: {result.get('count', 0)} invoices")
        return result
        
    except Exception as e:
        logging.error(f"Error fetching invoice history: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching invoice history: {str(e)}")


@app.post("/api/one-month-savings/calculate")
async def calculate_one_month_savings_endpoint(
    request: Request,
    authorization: str = Header(...),
):
    """Calculate 1-month savings from Member ACES Data sheet by identifier and utility type."""
    logging.info("=== One Month Savings Calculate Endpoint Called ===")
    request_data = await request.json()
    if authorization.startswith("Bearer "):
        token = authorization.split("Bearer ")[1]
        if token != os.getenv("BACKEND_API_KEY", "test-key"):
            try:
                verify_google_token(authorization)
            except Exception as e:
                logging.error(f"Token verification failed: {e}")
                raise HTTPException(status_code=401, detail="Invalid Google token")
    else:
        raise HTTPException(status_code=401, detail="Invalid authorization format")

    identifier = request_data.get("identifier")
    utility_type = request_data.get("utility_type")
    agreement_start_month = request_data.get("agreement_start_month")
    business_name = request_data.get("business_name")
    if not identifier or not utility_type or not agreement_start_month:
        raise HTTPException(
            status_code=400,
            detail="Missing required fields: identifier, utility_type, agreement_start_month",
        )
    try:
        result = calculate_one_month_savings(
            identifier=identifier,
            utility_type=utility_type,
            agreement_start_month=agreement_start_month,
            business_name=business_name,
        )
        return result
    except Exception as e:
        logging.exception("Error calculating one month savings")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/one-month-savings/status")
async def update_invoice_status_endpoint(
    request: Request,
    authorization: str = Header(...),
    user_info: dict = None
):
    """Update the status of a 1st Month Savings invoice (Generated / Sent / Paid)."""
    logging.info("=== One Month Savings Update Status Endpoint Called ===")
    request_data = await request.json()
    if authorization.startswith("Bearer "):
        token = authorization.split("Bearer ")[1]
        if token == os.getenv("BACKEND_API_KEY", "test-key"):
            user_info = {"email": request_data.get("user_email", "api_user@example.com")}
        else:
            try:
                user_info = verify_google_token(authorization)
            except Exception as e:
                logging.error(f"Token verification failed: {e}")
                raise HTTPException(status_code=401, detail="Invalid Google token")
    else:
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    business_name = request_data.get("business_name")
    invoice_number = request_data.get("invoice_number")
    status = request_data.get("status")
    if not business_name or not invoice_number or not status:
        raise HTTPException(
            status_code=400,
            detail="business_name, invoice_number and status are required"
        )
    result = update_invoice_status(business_name, invoice_number, status)
    if not result.get("success"):
        raise HTTPException(
            status_code=400 if "No matching" in str(result.get("error", "")) else 500,
            detail=result.get("error", "Failed to update status")
        )
    return result


@app.post("/api/one-month-savings/next-invoice-number")
async def get_next_invoice_number_endpoint(
    request: Request,
    authorization: str = Header(...),
    user_info: dict = None
):
    """Get the next sequential invoice number"""
    logging.info("=== One Month Savings Next Invoice Number Endpoint Called ===")
    
    # Get the request body
    request_data = await request.json()
    logging.info(f"Request data: {request_data}")
    
    # Check if it's an API key or Google token
    if authorization.startswith("Bearer "):
        token = authorization.split("Bearer ")[1]
        logging.info(f"Authorization token type: {'API Key' if token == os.getenv('BACKEND_API_KEY', 'test-key') else 'Google Token'}")
        
        # Check if it's a simple API key (for Next.js API routes)
        if token == os.getenv("BACKEND_API_KEY", "test-key"):
            # Use session email from request_data if available
            user_info = {"email": request_data.get("user_email", "api_user@example.com")}
            logging.info(f"Using API key authentication for user: {user_info.get('email')}")
        else:
            # Try to verify as Google token
            try:
                user_info = verify_google_token(authorization)
                logging.info(f"Google token verified for user: {user_info.get('email')}")
            except Exception as e:
                logging.error(f"Token verification failed: {e}")
                raise HTTPException(status_code=401, detail="Invalid Google token")
    else:
        logging.error("Invalid authorization format - missing 'Bearer ' prefix")
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    
    business_name = request_data.get("business_name")
    logging.info(f"Generating next invoice number (business: {business_name or 'all'})")
    
    try:
        invoice_number = get_next_sequential_invoice_number(business_name)
        
        result = {
            "invoice_number": invoice_number,
            "user_email": user_info.get("email")
        }
        
        logging.info(f"Generated next invoice number: {invoice_number}")
        return result
        
    except Exception as e:
        logging.error(f"Error generating invoice number: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error generating invoice number: {str(e)}")


class UploadInvoicePDFRequest(BaseModel):
    pdf_base64: str  # PDF as base64 encoded string
    filename: str
    client_folder_url: str
    invoice_number: str
    business_name: str

@app.post("/api/one-month-savings/upload-pdf")
async def upload_invoice_pdf_endpoint(
    request: Request,
    authorization: str = Header(...),
    user_info: dict = None
):
    """Upload invoice PDF to Google Drive folder"""
    logging.info("=== One Month Savings Upload PDF Endpoint Called ===")
    
    # Get the request body
    request_data = await request.json()
    logging.info(f"Request data keys: {list(request_data.keys())}")
    
    # For Drive upload, we accept either API key or Google access token
    # Access tokens are different from ID tokens - we don't verify them as ID tokens
    # Instead, we'll use the access token directly for Drive API calls
    if authorization.startswith("Bearer "):
        token = authorization.split("Bearer ")[1]
        logging.info(f"Authorization token type: {'API Key' if token == os.getenv('BACKEND_API_KEY', 'test-key') else 'Google Access Token'}")
        
        # Check if it's a simple API key (for Next.js API routes)
        if token == os.getenv("BACKEND_API_KEY", "test-key"):
            # Use session email from request_data if available
            user_info = {"email": request_data.get("user_email", "api_user@example.com")}
            logging.info(f"Using API key authentication for user: {user_info.get('email')}")
        else:
            # For access tokens, we don't verify as ID tokens
            # We'll validate the token when we use it with the Drive API
            # Extract user email from request data if available
            user_info = {"email": request_data.get("user_email", "unknown@example.com")}
            logging.info(f"Using Google access token for user: {user_info.get('email')}")
    else:
        logging.error("Invalid authorization format - missing 'Bearer ' prefix")
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    
    try:
        import base64
        
        pdf_base64 = request_data.get("pdf_base64")
        filename = request_data.get("filename")
        invoice_number = request_data.get("invoice_number")
        business_name = request_data.get("business_name")
        
        if not pdf_base64 or not filename:
            raise HTTPException(
                status_code=400,
                detail="Missing required fields: pdf_base64 and filename"
            )
        
        logging.info(f"Uploading PDF for invoice {invoice_number} (business: {business_name})")
        
        # Decode base64 PDF
        pdf_bytes = base64.b64decode(pdf_base64)
        logging.info(f"Decoded PDF, size: {len(pdf_bytes)} bytes")
        
        # Use fixed folder ID from environment variable or default
        from tools.one_month_savings import INVOICE_STORAGE_FOLDER_ID
        folder_id = INVOICE_STORAGE_FOLDER_ID
        
        if not folder_id:
            raise HTTPException(
                status_code=500,
                detail="Invoice storage folder ID not configured"
            )
        
        logging.info(f"Using invoice storage folder ID: {folder_id}")
        
        # Try user's OAuth token first (works for My Drive folders)
        # If that fails, fall back to service account (works for Shared Drives)
        file_id = None
        access_token = None
        refresh_token = request_data.get("refresh_token")
        
        logging.info(f"Authorization header present: {bool(authorization)}")
        logging.info(f"Refresh token present: {bool(refresh_token)}")
        
        if authorization.startswith("Bearer "):
            token = authorization.split("Bearer ")[1]
            if token != os.getenv("BACKEND_API_KEY", "test-key"):
                access_token = token
                logging.info(f"Access token extracted (length: {len(access_token) if access_token else 0})")
            else:
                logging.info("Token is API key, not user OAuth token")
        else:
            logging.warning("Authorization header does not start with 'Bearer '")
        
        # First, try with user's OAuth token (for My Drive folders)
        if access_token:
            try:
                logging.info("Attempting upload with user's OAuth token (for My Drive folders)")
                from google.oauth2.credentials import Credentials as UserCredentials
                
                client_id = os.getenv("GOOGLE_CLIENT_ID")
                client_secret = os.getenv("GOOGLE_CLIENT_SECRET")
                token_uri = "https://oauth2.googleapis.com/token"
                
                if not client_id or not client_secret:
                    logging.warning("Google OAuth credentials not configured, skipping user token attempt")
                else:
                    user_creds = UserCredentials(
                        token=access_token,
                        refresh_token=refresh_token,
                        token_uri=token_uri,
                        client_id=client_id,
                        client_secret=client_secret
                    )
                    user_drive_service = build('drive', 'v3', credentials=user_creds)
                    logging.info(f"Created Drive service with user credentials, attempting upload to folder {folder_id}")
                    file_id = upload_pdf_to_drive(pdf_bytes, filename, folder_id, user_drive_service)
                    if file_id:
                        logging.info("Successfully uploaded using user's OAuth token")
                    else:
                        logging.warning("Upload with user token returned None (check logs above for error details)")
            except Exception as e:
                logging.warning(f"Upload with user token failed with exception: {str(e)}")
                logging.exception(e)
                logging.info("Will try service account as fallback")
        else:
            logging.info("No access token available, skipping user token attempt")
        
        # If user token failed or wasn't available, try service account (for Shared Drives)
        if not file_id:
            logging.info("Attempting upload with service account (for Shared Drives)")
            drive_service = get_drive_service()
            
            if not drive_service:
                error_msg = (
                    "Failed to create Google Drive service. "
                    "Service account not configured properly. "
                    "Please check your service account credentials."
                )
                logging.error(error_msg)
                raise HTTPException(status_code=500, detail=error_msg)
            
            file_id = upload_pdf_to_drive(pdf_bytes, filename, folder_id, drive_service)
            
            if not file_id:
                # Provide helpful error message based on the error type
                user_email = request_data.get("user_email", "your Google account")
                error_msg = (
                    f"Failed to upload PDF to Google Drive. "
                    f"The invoice storage folder (ID: {folder_id}) is not accessible. "
                    f"To fix: Open the folder in Google Drive and share it with '{user_email}' with 'Editor' permissions. "
                    f"Alternatively, if using a Shared Drive, add the service account to the Shared Drive with 'Content Manager' role."
                )
                logging.error(error_msg)
                raise HTTPException(status_code=403, detail=error_msg)
        
        file_url = f"https://drive.google.com/file/d/{file_id}/view"
        
        logging.info(f"PDF uploaded successfully. File ID: {file_id}")
        logging.info(f"File ID type: {type(file_id)}")
        logging.info(f"File ID length: {len(file_id) if file_id else 0}")
        
        if not file_id:
            logging.error("❌ CRITICAL: Upload succeeded but file_id is None or empty!")
            raise HTTPException(
                status_code=500,
                detail="Upload succeeded but file_id was not returned"
            )
        
        response_data = {
            "success": True,
            "file_id": file_id,
            "file_url": file_url,
            "message": f"Invoice PDF uploaded successfully to {business_name}'s Google Drive"
        }
        
        logging.info(f"Returning upload response: {json.dumps({k: v for k, v in response_data.items() if k != 'message'}, indent=2)}")
        
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error uploading PDF: {str(e)}")
        logging.exception(e)
        raise HTTPException(status_code=500, detail=f"Error uploading PDF: {str(e)}")


# --- Testimonials API (member testimonials, optional link to 1st Month Savings) ---

TESTIMONIAL_STORAGE_FOLDER_ID = os.getenv("TESTIMONIAL_STORAGE_FOLDER_ID", "")


@app.get("/api/testimonials", response_model=List[TestimonialResponse])
async def list_testimonials(
    business_name: str = Query(..., description="Business name to list testimonials for"),
    authorization: str = Header(...),
    db: Session = Depends(get_db),
):
    """List testimonials for a business (by business_name)."""
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    token = authorization.split("Bearer ")[1]
    if token != os.getenv("BACKEND_API_KEY", "test-key"):
        try:
            verify_google_token(authorization)
        except Exception as e:
            logging.error(f"Token verification failed: {e}")
            raise HTTPException(status_code=401, detail="Invalid Google token")
    if not business_name or not business_name.strip():
        raise HTTPException(status_code=400, detail="business_name is required")
    items = db.query(Testimonial).filter(
        func.lower(Testimonial.business_name) == business_name.strip().lower()
    ).order_by(Testimonial.created_at.desc()).all()
    return [TestimonialResponse.model_validate(t) for t in items]


@app.get("/api/testimonials/check-approved", response_model=TestimonialCheckApprovedResponse)
async def check_testimonial_approved(
    business_name: str = Query(..., description="Business name to check"),
    authorization: str = Header(...),
    db: Session = Depends(get_db),
):
    """Check if the business has at least one Approved testimonial (for soft guard before 1st Month Savings invoice)."""
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    token = authorization.split("Bearer ")[1]
    if token != os.getenv("BACKEND_API_KEY", "test-key"):
        try:
            verify_google_token(authorization)
        except Exception as e:
            raise HTTPException(status_code=401, detail="Invalid Google token")
    if not business_name or not business_name.strip():
        raise HTTPException(status_code=400, detail="business_name is required")
    approved = db.query(Testimonial).filter(
        func.lower(Testimonial.business_name) == business_name.strip().lower(),
        Testimonial.status == "Approved",
    ).count()
    return TestimonialCheckApprovedResponse(has_approved=approved > 0, count=approved)


@app.post("/api/testimonials/upload", response_model=TestimonialResponse)
async def upload_testimonial(
    authorization: str = Header(...),
    file: UploadFile = File(...),
    business_name: str = Form(...),
    invoice_number: Optional[str] = Form(None),
    status: Optional[str] = Form("Draft"),
    gdrive_folder_url: Optional[str] = Form(None),
    testimonial_type: Optional[str] = Form(None),
    testimonial_solution_type_id: Optional[str] = Form(None),
    testimonial_savings: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    """
    Upload a testimonial document via n8n.

    Instead of uploading directly with the service account (which has no storage quota),
    this endpoint forwards the file + metadata to an n8n webhook, which handles the
    actual Drive upload and returns the created file ID. We then log that file as a
    Testimonial row in the CRM.
    """
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    token = authorization.split("Bearer ")[1]
    if token != os.getenv("BACKEND_API_KEY", "test-key"):
        try:
            verify_google_token(authorization)
        except Exception as e:
            raise HTTPException(status_code=401, detail="Invalid Google token")
    if not business_name or not business_name.strip():
        raise HTTPException(status_code=400, detail="business_name is required")
    filename = (file.filename or "document").strip()
    if not filename:
        raise HTTPException(status_code=400, detail="filename is required")
    allowed = (".pdf", ".docx", ".doc")
    if not any(filename.lower().endswith(ext) for ext in allowed):
        raise HTTPException(status_code=400, detail="File must be PDF or Word (.pdf, .docx, .doc)")
    status_val = (status or "Draft").strip()
    if status_val not in ("Draft", "Sent for approval", "Approved"):
        status_val = "Draft"

    # Prefer the explicit client folder URL; fall back to TESTIMONIAL_STORAGE_FOLDER_ID if set.
    drive_folder = (gdrive_folder_url or "").strip() or TESTIMONIAL_STORAGE_FOLDER_ID

    # Forward to n8n webhook which handles the actual Drive upload.
    try:
        contents = await file.read()
        files = {
            "file": (filename, contents, file.content_type or "application/octet-stream"),
        }
        norm_testimonial_type = (testimonial_type or "").strip()
        norm_solution_type_id = (testimonial_solution_type_id or "").strip()
        norm_savings = (testimonial_savings or "").strip()
        data = {
            "business_name": business_name.strip(),
            "drive_folder": drive_folder,
            "upload_type": "testimonial",
            # Always include these keys so they are visible in n8n even if blank
            "testimonial_type": norm_testimonial_type,
            "testimonial_solution_type_id": norm_solution_type_id,
            "testimonial_savings": norm_savings,
        }
        if invoice_number and invoice_number.strip():
            data["invoice_number"] = invoice_number.strip()

        logging.info(f"Calling testimonial-upload webhook with data: {data}")

        resp = requests.post(
            "https://membersaces.app.n8n.cloud/webhook/testimonial-upload",
            data=data,
            files=files,
            timeout=60,
        )
    except Exception as e:
        logging.error(f"Error calling testimonial-upload webhook: {e}")
        raise HTTPException(status_code=500, detail="Failed to call testimonial upload workflow.")

    if resp.status_code != 200:
        logging.error(f"testimonial-upload webhook failed: {resp.status_code} - {resp.text}")
        raise HTTPException(
            status_code=500,
            detail="Testimonial upload workflow failed.",
        )

    try:
        result_json = resp.json()
    except Exception:
        logging.error("testimonial-upload webhook did not return JSON.")
        raise HTTPException(status_code=500, detail="Testimonial upload workflow returned invalid response.")

    file_id = result_json.get("file_id")
    if not file_id:
        logging.error(f"testimonial-upload webhook missing file_id in response: {result_json}")
        raise HTTPException(status_code=500, detail="Testimonial upload workflow did not return file_id.")

    testimonial = Testimonial(
        business_name=business_name.strip(),
        file_name=filename,
        file_id=file_id,
        invoice_number=invoice_number.strip() if invoice_number and invoice_number.strip() else None,
        status=status_val,
        testimonial_type=norm_testimonial_type or None,
        testimonial_solution_type_id=norm_solution_type_id or None,
        testimonial_savings=norm_savings or None,
    )
    db.add(testimonial)
    db.commit()
    db.refresh(testimonial)
    return TestimonialResponse.model_validate(testimonial)


@app.patch("/api/testimonials/{testimonial_id}", response_model=TestimonialResponse)
async def update_testimonial(
    testimonial_id: int,
    body: TestimonialUpdate,
    authorization: str = Header(...),
    db: Session = Depends(get_db),
):
    """Update testimonial status and/or linked invoice number."""
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    token = authorization.split("Bearer ")[1]
    if token != os.getenv("BACKEND_API_KEY", "test-key"):
        try:
            verify_google_token(authorization)
        except Exception as e:
            raise HTTPException(status_code=401, detail="Invalid Google token")
    testimonial = db.query(Testimonial).filter(Testimonial.id == testimonial_id).first()
    if not testimonial:
        raise HTTPException(status_code=404, detail="Testimonial not found")
    if body.status is not None:
        if body.status.strip() in ("Draft", "Sent for approval", "Approved"):
            testimonial.status = body.status.strip()
    if body.invoice_number is not None:
        testimonial.invoice_number = body.invoice_number.strip() or None
    db.commit()
    db.refresh(testimonial)
    return TestimonialResponse.model_validate(testimonial)


# --- Testimonial solution content (defaults in code, overridable via API) ---


@app.get("/api/testimonials/solution-content", response_model=List[TestimonialSolutionContentItem])
async def list_testimonial_solution_content(
    solution_type: Optional[str] = Query(None, description="Filter to one solution type"),
    authorization: str = Header(...),
):
    """List merged testimonial content for all solution types, or one if solution_type is provided."""
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    token = authorization.split("Bearer ")[1]
    if token != os.getenv("BACKEND_API_KEY", "test-key"):
        try:
            verify_google_token(authorization)
        except Exception as e:
            logging.error(f"Token verification failed: {e}")
            raise HTTPException(status_code=401, detail="Invalid Google token")
    if solution_type:
        merged = get_merged_content(solution_type)
        if not merged:
            raise HTTPException(status_code=404, detail=f"Unknown solution_type: {solution_type}")
        return [TestimonialSolutionContentItem(**merged)]
    items = get_merged_content(None)
    return [TestimonialSolutionContentItem(**item) for item in items]


@app.put("/api/testimonials/solution-content", response_model=TestimonialSolutionContentItem)
async def update_testimonial_solution_content(
    request: Request,
    authorization: str = Header(...),
):
    """Save overrides for one solution type. Body: solution_type (required) + any content fields."""
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    token = authorization.split("Bearer ")[1]
    if token != os.getenv("BACKEND_API_KEY", "test-key"):
        try:
            verify_google_token(authorization)
        except Exception as e:
            logging.error(f"Token verification failed: {e}")
            raise HTTPException(status_code=401, detail="Invalid Google token")
    body = await request.json()
    st = body.get("solution_type")
    if not st or not isinstance(st, str):
        raise HTTPException(status_code=400, detail="solution_type is required")
    payload = {k: v for k, v in body.items() if k != "solution_type"}
    try:
        merged = save_override(st.strip(), payload)
        return TestimonialSolutionContentItem(**merged)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/testimonials/examples", response_model=List[TestimonialResponse])
async def get_testimonial_examples_for_solution_type(
    solution_type: str = Query(..., description="testimonial_solution_type_id to filter by"),
    limit: int = Query(5, ge=1, le=20),
    authorization: str = Header(...),
    db: Session = Depends(get_db),
):
    """Return recent testimonials for a given testimonial solution type (for content page examples)."""
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    token = authorization.split("Bearer ")[1]
    if token != os.getenv("BACKEND_API_KEY", "test-key"):
        try:
            verify_google_token(authorization)
        except Exception as e:
            logging.error(f"Token verification failed: {e}")
            raise HTTPException(status_code=401, detail="Invalid Google token")
    items = get_testimonials_for_solution_type(db, solution_type_id=solution_type, limit=limit)
    return [TestimonialResponse.model_validate(t) for t in items]

@app.post("/api/testimonials/generate-document")
async def generate_testimonial_document_endpoint(
    request: Request,
    authorization: str = Header(...),
    db: Session = Depends(get_db),
):
    """Generate a testimonial document from the template via n8n. Body: business info + solution_type + savings_amount."""
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    token = authorization.split("Bearer ")[1]
    if token != os.getenv("BACKEND_API_KEY", "test-key"):
        try:
            verify_google_token(authorization)
        except Exception as e:
            logging.error(f"Token verification failed: {e}")
            raise HTTPException(status_code=401, detail="Invalid Google token")
    body = await request.json()
    business_name = (body.get("business_name") or "").strip()
    solution_type_id = (body.get("solution_type") or "").strip()
    savings_amount = body.get("savings_amount")
    if not business_name or not solution_type_id:
        raise HTTPException(status_code=400, detail="business_name and solution_type are required")
    try:
        savings_val = float(savings_amount) if savings_amount is not None else 0.0
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="savings_amount must be a number")
    result = generate_testimonial_document(
        business_name=business_name,
        trading_as=(body.get("trading_as") or "").strip(),
        contact_name=(body.get("contact_name") or "").strip(),
        position=(body.get("position") or "").strip(),
        email=(body.get("email") or "").strip(),
        telephone=(body.get("telephone") or "").strip(),
        client_folder_url=(body.get("client_folder_url") or "").strip(),
        solution_type_id=solution_type_id,
        savings_amount=savings_val,
        abn=(body.get("abn") or "").strip(),
        postal_address=(body.get("postal_address") or "").strip(),
        site_address=(body.get("site_address") or "").strip(),
    )
    if result.get("status") == "error":
        raise HTTPException(
            status_code=500 if "timeout" in (result.get("message") or "").lower() else 400,
            detail=result.get("message", "Generation failed"),
        )
    # If n8n returned a document link, try to log this as a Testimonial row in the CRM.
    document_link = result.get("document_link")
    if document_link:
        try:
            # Reuse folder-ID extraction helper; it also handles /d/ and ?id= patterns for files.
            file_id = extract_folder_id_from_url(document_link)
            if file_id:
                file_name = f"Testimonial - {business_name}" if business_name else "Testimonial"
                testimonial = Testimonial(
                    business_name=business_name.strip(),
                    file_name=file_name,
                    file_id=file_id,
                    invoice_number=None,
                    status="Draft",
                    testimonial_type=result.get("testimonial_type") or None,
                    testimonial_solution_type_id=solution_type_id or None,
                    testimonial_savings=str(savings_val) if savings_val is not None else None,
                )
                db.add(testimonial)
                db.commit()
                db.refresh(testimonial)
                result["testimonial_id"] = testimonial.id
        except Exception as e:
            logging.error(f"Failed to create Testimonial record from generated document: {e}")
    return result


# Task API Routes
@app.post("/api/tasks", response_model=TaskResponse)
async def create_task(
    task: TaskCreate,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db)
):
    """Create a new task"""
    logging.info(f"Received task creation request: {task.title}")
    
    user_info = user_data["idinfo"]
    current_user_email = user_info.get("email")
    
    # Use the authenticated user's email as assigned_by if not provided
    assigned_by = task.assigned_by or current_user_email
    
    db_task = Task(
        title=task.title,
        description=task.description,
        due_date=task.due_date,
        status=task.status,
        assigned_to=task.assigned_to,
        assigned_by=assigned_by,
        business_id=task.business_id,
        client_id=task.client_id,
        category=task.category or "task",
    )
    
    db.add(db_task)
    db.commit()
    db.refresh(db_task)
    
    logging.info(f"Task created successfully: {db_task.id}")
    
    # Log task creation in history
    log_task_created(db, db_task.id, current_user_email)
    
    # Send email notification for new task
    if task.assigned_to:
        try:
            await send_new_task_email(task.assigned_to, assigned_by, db_task, db)
        except Exception as e:
            logging.error(f"Failed to send new task email: {str(e)}")
    
    return db_task


@app.get("/api/tasks/my", response_model=List[TaskResponse])
def get_my_tasks(
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db)
):
    """Get all tasks assigned to the current user"""
    user_info = user_data["idinfo"]
    user_email = user_info.get("email")
    logging.info(f"Fetching tasks for user: {user_email}")
    
    tasks = db.query(Task).filter(Task.assigned_to == user_email).all()
    
    logging.info(f"Found {len(tasks)} tasks for user {user_email}")
    return tasks


@app.patch("/api/tasks/{task_id}/status", response_model=TaskResponse)
async def update_task_status(
    task_id: int,
    status_update: TaskStatusUpdate,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db)
):
    """Update the status of a task"""
    logging.info(f"Updating task {task_id} status to: {status_update.status}")
    
    user_info = user_data["idinfo"]
    current_user_email = user_info.get("email")
    
    db_task = db.query(Task).filter(Task.id == task_id).first()
    
    if not db_task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Permission check: only assigned user or creator can update
    if db_task.assigned_to != current_user_email and db_task.assigned_by != current_user_email:
        raise HTTPException(status_code=403, detail="You may only edit tasks assigned to you or created by you.")
    
    old_status = db_task.status
    db_task.status = status_update.status
    db.commit()
    db.refresh(db_task)
    
    logging.info(f"Task {task_id} status updated successfully")
    
    # Log status change in history
    log_status_change(
        db, task_id, current_user_email,
        old_status, status_update.status
    )
    
    # Send email notification if task is marked as completed
    if status_update.status.lower() == "completed" and old_status.lower() != "completed":
        if db_task.assigned_by and db_task.assigned_to:
            try:
                await send_task_completed_email(
                    db_task.assigned_by,
                    db_task.assigned_to,
                    db_task,
                    db
                )
            except Exception as e:
                logging.error(f"Failed to send task completed email: {str(e)}")
    
    return db_task


@app.patch("/api/tasks/{task_id}", response_model=TaskResponse)
async def update_task(
    task_id: int,
    task_update: TaskUpdate,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db)
):
    """Update task fields (title, description, due_date, assigned_to, business_id, client_id, category)"""
    logging.info(f"Updating task {task_id}")
    
    user_info = user_data["idinfo"]
    current_user_email = user_info.get("email")
    
    db_task = db.query(Task).filter(Task.id == task_id).first()
    
    if not db_task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Permission check: only assigned user or creator can update
    if db_task.assigned_to != current_user_email and db_task.assigned_by != current_user_email:
        raise HTTPException(status_code=403, detail="You may only edit tasks assigned to you or created by you.")
    
    # If task is completed and being edited, reset to in_progress
    if db_task.status.lower() == "completed":
        log_field_change(
            db, task_id, current_user_email,
            "title", db_task.title, task_update.title
        )
        db_task.status = "in_progress"
        logging.info(f"Task {task_id} status reset from 'completed' to 'in_progress' due to edit")
    
    # Update title
    if task_update.title is not None and task_update.title != db_task.title:
        log_field_change(
            db, task_id, current_user_email,
            "title", db_task.title, task_update.title
        )
        db_task.title = task_update.title

    # Update description
    if task_update.description is not None and task_update.description != db_task.description:
        log_field_change(
            db, task_id, current_user_email,
            "description", db_task.description, task_update.description
        )
        db_task.description = task_update.description

    # Update due_date
    if task_update.due_date is not None and task_update.due_date != db_task.due_date:
        old_due_date_str = db_task.due_date.strftime("%Y-%m-%d %H:%M:%S") if db_task.due_date else None
        new_due_date_str = task_update.due_date.strftime("%Y-%m-%d %H:%M:%S") if task_update.due_date else None
        log_field_change(
            db, task_id, current_user_email,
            "due_date", old_due_date_str, new_due_date_str
        )
        db_task.due_date = task_update.due_date

    # Update assigned_to
    if task_update.assigned_to is not None and task_update.assigned_to != db_task.assigned_to:
        log_field_change(
            db, task_id, current_user_email,
            "assigned_to", db_task.assigned_to, task_update.assigned_to
        )
        db_task.assigned_to = task_update.assigned_to

    # Update business_id
    if task_update.business_id is not None and task_update.business_id != db_task.business_id:
        log_field_change(
            db, task_id, current_user_email,
            "business_id", str(db_task.business_id), str(task_update.business_id)
        )
        db_task.business_id = task_update.business_id

    # Update client_id
    if task_update.client_id is not None and task_update.client_id != db_task.client_id:
        log_field_change(
            db, task_id, current_user_email,
            "client_id", str(db_task.client_id), str(task_update.client_id)
        )
        db_task.client_id = task_update.client_id

    # Update category
    if task_update.category is not None and task_update.category != db_task.category:
        log_field_change(
            db, task_id, current_user_email,
            "category", db_task.category, task_update.category
        )
        db_task.category = task_update.category

    
    db.commit()
    db.refresh(db_task)
    
    logging.info(f"Task {task_id} updated successfully")
    return db_task


@app.get("/api/tasks/by-business/{business_id}", response_model=List[TaskResponse])
def get_tasks_by_business(
    business_id: int,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db)
):
    """Get all tasks for a specific business"""
    logging.info(f"Fetching tasks for business_id: {business_id}")
    
    tasks = db.query(Task).filter(Task.business_id == business_id).all()
    
    logging.info(f"Found {len(tasks)} tasks for business_id {business_id}")
    return tasks


@app.get("/api/clients/{client_id}/tasks", response_model=List[TaskResponse])
def get_tasks_by_client(
    client_id: int,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Get all tasks for a specific client"""
    logging.info(f"Fetching tasks for client_id: {client_id}")

    tasks = db.query(Task).filter(Task.client_id == client_id).all()

    logging.info(f"Found {len(tasks)} tasks for client_id {client_id}")
    return tasks


@app.get("/api/users", response_model=List[UserResponse])
def list_users(
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db)
):
    """Get all users (for assign dropdown)"""
    logging.info("Fetching all users")
    
    users = db.query(User).all()
    
    logging.info(f"Found {len(users)} users")
    return users

@app.get("/api/tasks/assigned-by-me", response_model=List[TaskResponse])
def get_tasks_assigned_by_me(
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db)
):
    """Get all tasks created by the current user"""
    user_info = user_data["idinfo"]
    user_email = user_info.get("email")
    logging.info(f"Fetching tasks assigned by user: {user_email}")
    
    tasks = db.query(Task).filter(Task.assigned_by == user_email).all()
    
    logging.info(f"Found {len(tasks)} tasks assigned by {user_email}")
    return tasks


@app.delete("/api/tasks/{task_id}")
def delete_task(
    task_id: int,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db)
):
    """Delete a task"""
    logging.info(f"Deleting task {task_id}")
    
    user_info = user_data["idinfo"]
    current_user_email = user_info.get("email")
    
    db_task = db.query(Task).filter(Task.id == task_id).first()
    
    if not db_task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Permission check: only assigned user or creator can delete
    if db_task.assigned_to != current_user_email and db_task.assigned_by != current_user_email:
        raise HTTPException(status_code=403, detail="You may only delete tasks assigned to you or created by you.")
    
    # Log deletion in history before deleting
    log_task_deleted(db, task_id, current_user_email)
    
    # Delete the task (history will be kept due to foreign key, or cascade if configured)
    db.delete(db_task)
    db.commit()
    
    logging.info(f"Task {task_id} deleted successfully by {current_user_email}")
    return {"status": "success", "message": f"Task {task_id} deleted successfully"}


@app.get("/api/tasks/all", response_model=List[TaskResponse])
def get_all_tasks(
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db)
):
    """Get all tasks"""
    logging.info("Fetching all tasks")
    
    tasks = db.query(Task).all()
    
    logging.info(f"Found {len(tasks)} tasks")
    return tasks


@app.get("/api/tasks/{task_id}/history")
def get_task_history(
    task_id: int,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
    page: int = Query(1, ge=1, description="Page number (starts at 1)"),
    page_size: int = Query(50, ge=1, le=100, description="Number of items per page")
):
    """Get history for a specific task with pagination and batched edit grouping"""
    from utils.timezone import to_melbourne_iso
    from datetime import timedelta
    
    logging.info(f"Fetching history for task {task_id}, page {page}, page_size {page_size}")
    
    # Verify task exists
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Get total count
    total_count = db.query(TaskHistory).filter(
        TaskHistory.task_id == task_id
    ).count()
    
    # Get history ordered by creation time (descending for pagination)
    offset = (page - 1) * page_size
    history = db.query(TaskHistory).filter(
        TaskHistory.task_id == task_id
    ).order_by(TaskHistory.created_at.desc()).offset(offset).limit(page_size).all()
    
    # Group batched edits (edits by same user within 5 seconds)
    grouped_history = []
    BATCH_WINDOW_SECONDS = 5
    
    for i, h in enumerate(history):
        if i == 0:
            # First item starts a new group
            current_group = {
                "id": h.id,
                "action": h.action,
                "fields": [{"field": h.field, "old_value": h.old_value, "new_value": h.new_value}] if h.field else [],
                "user_email": h.user_email,
                "created_at": to_melbourne_iso(h.created_at),
                "is_batched": False
            }
            grouped_history.append(current_group)
        else:
            prev_h = history[i - 1]
            time_diff = (prev_h.created_at - h.created_at).total_seconds()
            
            # Check if this edit should be grouped with previous
            should_group = (
                h.action == "field_updated" and
                prev_h.action == "field_updated" and
                h.user_email == prev_h.user_email and
                time_diff <= BATCH_WINDOW_SECONDS
            )
            
            if should_group:
                # Add to previous group
                current_group["fields"].append({
                    "field": h.field,
                    "old_value": h.old_value,
                    "new_value": h.new_value
                })
                current_group["is_batched"] = True
            else:
                # Start new group
                current_group = {
                    "id": h.id,
                    "action": h.action,
                    "fields": [{"field": h.field, "old_value": h.old_value, "new_value": h.new_value}] if h.field else [],
                    "user_email": h.user_email,
                    "created_at": to_melbourne_iso(h.created_at),
                    "is_batched": False
                }
                grouped_history.append(current_group)
    
    # Reverse to show oldest first
    grouped_history.reverse()
    
    # ---- GROUP BY DATE FOR FRONTEND ----
    from collections import defaultdict
    from datetime import datetime

    date_groups = defaultdict(list)

    for item in grouped_history:
        # item['created_at'] is already ISO Melbourne string
        dt = datetime.fromisoformat(item["created_at"])
        date_key = dt.strftime("%B %d, %Y")   # e.g. “November 21, 2025”
        date_groups[date_key].append(item)

    groups_list = [
        {"date": date, "items": items}
        for date, items in sorted(
            date_groups.items(),
            key=lambda x: datetime.strptime(x[0], "%B %d, %Y"),
            reverse=True
        )
    ]

    return {
        "groups": groups_list,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total": total_count,
            "has_next": page * page_size < total_count,
            "has_prev": page > 1
        }
    }


@app.post("/api/tasks/check-due")
async def check_due_tasks_endpoint(
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db)
):
    """Manually trigger check for due/overdue tasks (for testing)"""
    logging.info("Manual due tasks check triggered")
    
    try:
        await check_due_tasks(db)
        return {
            "status": "success",
            "message": "Due tasks check completed. Check logs for details."
        }
    except Exception as e:
        logging.error(f"Error during due tasks check: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error checking due tasks: {str(e)}")


# Client Status Note endpoints
@app.post("/api/client-status", response_model=ClientStatusNoteResponse)
def create_client_status_note(
    note: ClientStatusNoteCreate,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db)
):
    """Create a new client status note"""
    user_info = user_data["idinfo"]
    user_email = user_info.get("email")
    
    logging.info(f"Creating client status note for {note.business_name}")
    
    db_note = ClientStatusNote(
        business_name=note.business_name,
        client_id=note.client_id,
        note=note.note,
        user_email=user_email,
        note_type=note.note_type or "general",
        related_task_id=note.related_task_id,
    )
    
    db.add(db_note)
    db.commit()
    db.refresh(db_note)
    
    logging.info(f"Client status note created: {db_note.id}")
    return db_note


@app.get("/api/client-status/{business_name}", response_model=List[ClientStatusNoteResponse])
def get_client_status_notes(
    business_name: str,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db)
):
    """Get all status notes for a specific business"""
    logging.info(f"Fetching client status notes for {business_name}")
    
    notes = db.query(ClientStatusNote).filter(
        ClientStatusNote.business_name == business_name
    ).order_by(ClientStatusNote.created_at.desc()).all()
    
    logging.info(f"Found {len(notes)} notes for {business_name}")
    return notes


@app.patch("/api/client-status/{note_id}", response_model=ClientStatusNoteResponse)
def update_client_status_note(
    note_id: int,
    note_update: ClientStatusNoteUpdate,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db)
):
    """Update a client status note"""
    user_info = user_data["idinfo"]
    
    logging.info(f"Updating client status note {note_id}")
    
    db_note = db.query(ClientStatusNote).filter(ClientStatusNote.id == note_id).first()
    
    if not db_note:
        raise HTTPException(status_code=404, detail="Note not found")
    
    db_note.note = note_update.note
    if note_update.note_type is not None:
        db_note.note_type = note_update.note_type
    db.commit()
    db.refresh(db_note)
    
    logging.info(f"Client status note {note_id} updated")
    return db_note

@app.delete("/api/client-status/{note_id}", response_model=dict)
def delete_client_status_note(
    note_id: int,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db)
):
    """Delete a client status note"""
    logging.info(f"Deleting client status note {note_id}")
    
    db_note = db.query(ClientStatusNote).filter(ClientStatusNote.id == note_id).first()
    
    if not db_note:
        raise HTTPException(status_code=404, detail="Note not found")
    
    db.delete(db_note)
    db.commit()
    
    logging.info(f"Client status note {note_id} deleted")
    return {"status": "success", "message": "Note deleted"}


# ---------------------------------------------------------------------------
# Strategy & WIP – per-client strategy items
# ---------------------------------------------------------------------------


def _format_date_for_csv(value):
    """Format datetime or date string for CSV (YYYY-MM-DD)."""
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    s = str(value).strip()
    if len(s) >= 10:
        return s[:10]
    return s


def _float_for_csv(value):
    """Format float for CSV; empty if None."""
    if value is None:
        return ""
    try:
        return str(float(value))
    except (TypeError, ValueError):
        return ""


def _str_for_csv(value):
    """String for CSV; empty if None."""
    return "" if value is None else str(value).strip()


def _build_strategy_wip_csv(items: List, client, year: int) -> str:
    """
    Build Strategy & WIP CSV in the same format as the boss's template.
    items: list of StrategyItem for this client/year, ordered by section, row_index.
    client: Client model (for business_name).
    """
    from datetime import datetime
    buf = io.StringIO()
    writer = csv.writer(buf)
    # Empty rows 1–4
    for _ in range(4):
        writer.writerow([""] * 13)
    # Header
    business_name = (client.business_name or "Business Member Detail").strip()
    writer.writerow([business_name] + [""] * 12)
    now = datetime.utcnow()
    writer.writerow([f"Strategy {year} Update {now.strftime('%m')}.{year}"] + [""] * 12)
    writer.writerow([""] * 13)

    section_order = [
        "past_achievements_annual",
        "in_progress",
        "objective",
        "advocate",
        "summary",
    ]
    section_titles = {
        "past_achievements_annual": "▶  Past Achievements Annual",
        "in_progress": "▶  In Progress",
        "objective": "▶  Objective",
        "advocate": "▶  Advocate",
        "summary": "▶  Summary",
    }
    past_achievements_header = [
        "Member Level / Solutions",
        "Details",
        "SDG",
        "Key results",
        "Solutions Details",
        "Solutions Details",
        "Solutions Details",
        " Saving Achieved",
        " New Revenue Achieved",
        "Saving Start Date",
        " New Revenue  Start Date",
        "Priority",
        "Status",
    ]
    in_progress_header = [
        "Member Level / Solutions",
        "Solution type",
        "SDG",
        "Key results",
        "Engagement Form ",
        "Contract Signed",
        "Est. saving achieved p.a.",
        "Est. revenue achieved p.a.",
        "Est. sav/rev over duration",
        "Est. start date",
        "Est. sav. KPI achieved",
        "Priority",
        "Status",
    ]

    by_section = {}
    for item in items:
        key = (item.section or "").strip() or "in_progress"
        if key not in by_section:
            by_section[key] = []
        by_section[key].append(item)

    for section_key in section_order:
        writer.writerow([section_titles.get(section_key, f"▶  {section_key}")] + [""] * 12)
        rows = by_section.get(section_key, [])
        rows = sorted(rows, key=lambda r: (r.row_index, r.id))

        if section_key == "past_achievements_annual":
            writer.writerow(past_achievements_header)
            total_saving = 0.0
            total_revenue = 0.0
            for r in rows:
                s_ach = r.saving_achieved if r.saving_achieved is not None else 0
                r_ach = r.new_revenue_achieved if r.new_revenue_achieved is not None else 0
                total_saving += float(s_ach)
                total_revenue += float(r_ach)
                writer.writerow([
                    _str_for_csv(r.member_level_solutions),
                    _str_for_csv(r.details),
                    _str_for_csv(r.sdg),
                    _str_for_csv(r.key_results),
                    _str_for_csv(r.solution_details_1),
                    _str_for_csv(r.solution_details_2),
                    _str_for_csv(r.solution_details_3),
                    _float_for_csv(r.saving_achieved),
                    _float_for_csv(r.new_revenue_achieved),
                    _format_date_for_csv(r.saving_start_date),
                    _format_date_for_csv(r.new_revenue_start_date),
                    _str_for_csv(r.priority),
                    _str_for_csv(r.status),
                ])
            writer.writerow(["", "", "", "Net Total", "", "", "", _float_for_csv(total_saving), _float_for_csv(total_revenue), "", "", "", ""])
        else:
            writer.writerow(in_progress_header)
            total_sav = 0.0
            total_rev = 0.0
            total_dur = 0.0
            for r in rows:
                s = r.est_saving_pa if r.est_saving_pa is not None else 0
                rev = r.est_revenue_pa if r.est_revenue_pa is not None else 0
                d = r.est_sav_rev_over_duration if r.est_sav_rev_over_duration is not None else 0
                total_sav += float(s)
                total_rev += float(rev)
                total_dur += float(d)
                writer.writerow([
                    _str_for_csv(r.member_level_solutions),
                    _str_for_csv(r.solution_type),
                    _str_for_csv(r.sdg),
                    _str_for_csv(r.key_results),
                    _str_for_csv(r.engagement_form),
                    _str_for_csv(r.contract_signed),
                    _float_for_csv(r.est_saving_pa),
                    _float_for_csv(r.est_revenue_pa),
                    _float_for_csv(r.est_sav_rev_over_duration),
                    _format_date_for_csv(r.est_start_date),
                    _str_for_csv(r.est_sav_kpi_achieved),
                    _str_for_csv(r.priority),
                    _str_for_csv(r.status),
                ])
            writer.writerow(["", "", "", "Net Total", "", "", _float_for_csv(total_sav), _float_for_csv(total_rev), _float_for_csv(total_dur), "", "", "", ""])

    return buf.getvalue()


@app.get(
    "/api/clients/{client_id}/strategy-items/export-csv",
    response_class=Response,
)
def export_strategy_items_csv(
    client_id: int,
    year: int = Query(..., description="Strategy year to export"),
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """
    Download Strategy & WIP for the client and year as CSV in the boss's template format.
    """
    client = db.query(Client).filter(Client.id == client_id).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    query = db.query(StrategyItem).filter(StrategyItem.client_id == client_id, StrategyItem.year == year)
    # Exclude items hidden from WIP for CSV export
    query = query.filter(
        (StrategyItem.excluded_from_wip == 0) | (StrategyItem.excluded_from_wip.is_(None))
    )
    items = (
        query.order_by(
            StrategyItem.section.asc(),
            StrategyItem.row_index.asc(),
            StrategyItem.id.asc(),
        )
        .all()
    )
    csv_content = _build_strategy_wip_csv(items, client, year)
    filename = f"Strategy-WIP-{client_id}-{year}.csv"
    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get(
    "/api/clients/{client_id}/strategy-items",
    response_model=List[StrategyItemResponse],
)
def list_strategy_items_for_client(
    client_id: int,
    year: Optional[int] = Query(None, description="Filter by strategy year"),
    excluded: Optional[int] = Query(0, description="0 = only included in WIP (default), 1 = only excluded (removed from WIP)"),
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """
    List Strategy & WIP items for a client (optionally filtered by year).
    By default returns only items included in WIP (excluded_from_wip=0).
    Use excluded=1 to list items that were "removed from WIP" (so UI can show "Include in WIP").
    """
    logging.info(f"Listing strategy items for client_id={client_id}, year={year}, excluded={excluded}")

    query = db.query(StrategyItem).filter(StrategyItem.client_id == client_id)
    if year is not None:
        query = query.filter(StrategyItem.year == year)
    if excluded == 1:
        query = query.filter(StrategyItem.excluded_from_wip == 1)
    else:
        query = query.filter(
            (StrategyItem.excluded_from_wip == 0) | (StrategyItem.excluded_from_wip.is_(None))
        )

    items = (
        query.order_by(
            StrategyItem.section.asc(),
            StrategyItem.row_index.asc(),
            StrategyItem.id.asc(),
        )
        .all()
    )
    return items


@app.post(
    "/api/clients/{client_id}/strategy-items/sync-from-crm",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
def sync_strategy_items_from_crm_endpoint(
    client_id: int,
    year: Optional[int] = Query(None, description="Strategy year to backfill (default: current year)"),
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """
    Backfill Strategy & WIP from this client's existing offers and activities.
    Creates a strategy row for each relevant activity (engagement form, comparison,
    DMA review, etc.) that does not already have one. Idempotent for existing rows.
    """
    logging.info("Syncing strategy items from CRM for client_id=%s, year=%s", client_id, year)
    created = sync_strategy_items_from_crm(db, client_id=client_id, year=year)
    db.commit()
    logging.info("Sync from CRM created %s strategy items for client_id=%s", created, client_id)
    return {"created": created, "message": f"Added {created} row(s) from existing offers and activities."}


@app.post(
    "/api/clients/{client_id}/strategy-items",
    response_model=StrategyItemResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_strategy_item_for_client(
    client_id: int,
    item: StrategyItemCreate,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """
    Create a new Strategy & WIP item for a client.

    The client_id is taken from the path; the body describes the row contents.
    """
    logging.info(
        "Creating strategy item for client_id=%s, section=%s, year=%s",
        client_id,
        item.section,
        item.year,
    )

    db_item = StrategyItem(
        client_id=client_id,
        year=item.year,
        section=item.section,
        row_index=item.row_index,
        member_level_solutions=item.member_level_solutions,
        details=item.details,
        solution_type=item.solution_type,
        sdg=item.sdg,
        key_results=item.key_results,
        solution_details_1=item.solution_details_1,
        solution_details_2=item.solution_details_2,
        solution_details_3=item.solution_details_3,
        engagement_form=item.engagement_form,
        contract_signed=item.contract_signed,
        saving_achieved=item.saving_achieved,
        new_revenue_achieved=item.new_revenue_achieved,
        est_saving_pa=item.est_saving_pa,
        est_revenue_pa=item.est_revenue_pa,
        est_sav_rev_over_duration=item.est_sav_rev_over_duration,
        saving_start_date=item.saving_start_date,
        new_revenue_start_date=item.new_revenue_start_date,
        est_start_date=item.est_start_date,
        est_sav_kpi_achieved=item.est_sav_kpi_achieved,
        priority=item.priority,
        status=item.status,
    )

    db.add(db_item)
    db.commit()
    db.refresh(db_item)

    logging.info("Created strategy item id=%s for client_id=%s", db_item.id, client_id)
    return db_item


@app.patch(
    "/api/strategy-items/{item_id}",
    response_model=StrategyItemResponse,
)
def update_strategy_item(
    item_id: int,
    update: StrategyItemUpdate,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """
    Update an existing Strategy & WIP item.
    When est_saving_pa or saving_achieved is updated and the item is linked to an offer,
    the offer's annual_savings is updated so offer CRM and WIP stay in sync.
    """
    logging.info("Updating strategy item id=%s", item_id)

    db_item = db.query(StrategyItem).filter(StrategyItem.id == item_id).first()
    if not db_item:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    update_data = update.model_dump(exclude_unset=True)
    # Coerce excluded_from_wip bool to 0/1 for SQLite
    if "excluded_from_wip" in update_data:
        update_data["excluded_from_wip"] = 1 if update_data["excluded_from_wip"] else 0

    for field, value in update_data.items():
        setattr(db_item, field, value)

    # Cross-sync: if this item is linked to an offer and we updated savings, push to offer
    if db_item.offer_id and ("est_saving_pa" in update_data or "saving_achieved" in update_data):
        offer = db.query(Offer).filter(Offer.id == db_item.offer_id).first()
        if offer:
            # Prefer est_saving_pa (in-progress) else saving_achieved (past)
            val = db_item.est_saving_pa if "est_saving_pa" in update_data else db_item.saving_achieved
            if val is not None:
                try:
                    offer.annual_savings = float(val)
                except (TypeError, ValueError):
                    pass
            elif "est_saving_pa" in update_data or "saving_achieved" in update_data:
                offer.annual_savings = None

    db.commit()
    db.refresh(db_item)

    logging.info("Updated strategy item id=%s", item_id)
    return db_item


@app.delete(
    "/api/strategy-items/{item_id}",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
def delete_strategy_item(
    item_id: int,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """
    Delete a Strategy & WIP item.
    """
    logging.info("Deleting strategy item id=%s", item_id)

    db_item = db.query(StrategyItem).filter(StrategyItem.id == item_id).first()
    if not db_item:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    db.delete(db_item)
    db.commit()

    logging.info("Deleted strategy item id=%s", item_id)
    return {"status": "success", "message": "Strategy item deleted"}

@app.get("/api/client-status/debug/all")
def debug_all_notes(db: Session = Depends(get_db)):
    """Debug endpoint to see all notes"""
    notes = db.query(ClientStatusNote).all()
    return {
        "count": len(notes), 
        "notes": [{
            "id": n.id, 
            "business_name": n.business_name, 
            "client_id": n.client_id,
            "note": n.note[:100],
            "user_email": n.user_email,
            "note_type": n.note_type,
            "created_at": str(n.created_at)
        } for n in notes]
    }


@app.post("/api/tasks/check-due-cron")
async def check_due_tasks_cron(db: Session = Depends(get_db)):
    """Cron endpoint for Cloud Scheduler - no auth required"""
    logging.info("Cron job triggered: checking due tasks")
    
    try:
        await check_due_tasks(db)
        return {
            "status": "success",
            "message": "Due tasks check completed"
        }
    except Exception as e:
        logging.error(f"Error during due tasks check: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ---- CRM: Clients, Offers, and Pipeline ----


@app.post("/api/clients", response_model=ClientResponse)
def create_client(
    client: ClientCreate,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Create a new client record"""
    logging.info(f"Creating client: {client.business_name}")

    existing = (
        db.query(Client)
        .filter(Client.business_name == client.business_name)
        .first()
    )
    if existing:
        raise HTTPException(
            status_code=400,
            detail="Client with this business name already exists",
        )

    db_client = Client(
        business_name=client.business_name,
        external_business_id=client.external_business_id,
        primary_contact_email=client.primary_contact_email,
        gdrive_folder_url=client.gdrive_folder_url,
        stage=client.stage or "lead",
        owner_email=client.owner_email or (user_data.get("idinfo") or {}).get("email"),
    )
    db.add(db_client)
    db.commit()
    db.refresh(db_client)

    logging.info(f"Client created with id {db_client.id}")
    return db_client


@app.get("/api/clients")
def list_clients(
    query: Optional[str] = Query(None, description="Search by business name (partial match)"),
    stage: Optional[str] = Query(None, description="Filter by client stage"),
    created_after: Optional[str] = Query(None, description="Filter clients created on or after date (YYYY-MM-DD)"),
    created_before: Optional[str] = Query(None, description="Filter clients created on or before date (YYYY-MM-DD)"),
    mine: Optional[bool] = Query(None, description="If true, only clients where owner_email matches current user"),
    limit: Optional[int] = Query(None, description="Max number of clients to return (enables paginated response with total)"),
    offset: Optional[int] = Query(None, description="Number of clients to skip (use with limit)"),
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """
    List clients. Optional filters: query, stage, created_after, created_before, mine (My clients).
    When limit (or offset) is set, returns { "items": [...], "total": N }; otherwise returns a plain list (backward compatible).
    """
    from datetime import datetime as dt
    from datetime import timedelta
    logging.info("Listing clients")
    q = (query or "").strip()

    base_query = db.query(Client)
    if q:
        pattern = f"%{q}%"
        base_query = base_query.filter(Client.business_name.ilike(pattern))
    if stage is not None and stage.strip():
        base_query = base_query.filter(Client.stage == stage.strip())
    if mine:
        user_email = (user_data.get("idinfo") or {}).get("email")
        if user_email:
            base_query = base_query.filter(Client.owner_email == user_email)
    if created_after:
        try:
            start = dt.strptime(created_after, "%Y-%m-%d")
            base_query = base_query.filter(Client.created_at >= start)
        except ValueError:
            pass
    if created_before:
        try:
            end = dt.strptime(created_before, "%Y-%m-%d")
            end_inclusive = end + timedelta(days=1)
            base_query = base_query.filter(Client.created_at < end_inclusive)
        except ValueError:
            pass

    ordered = base_query.order_by(Client.business_name.asc())
    if limit is not None or offset is not None:
        total = ordered.count()
        off = offset or 0
        lim = limit or 20
        clients = ordered.offset(off).limit(lim).all()
        items = [ClientResponse.model_validate(c).model_dump(mode="json") for c in clients]
        return JSONResponse(content={"items": items, "total": total})
    clients = ordered.all()
    return [ClientResponse.model_validate(c) for c in clients]


@app.get("/api/clients/export")
def export_clients_csv(
    query: Optional[str] = Query(None, description="Search by business name (partial match)"),
    stage: Optional[str] = Query(None, description="Filter by client stage"),
    created_after: Optional[str] = Query(None, description="Filter clients created on or after date (YYYY-MM-DD)"),
    created_before: Optional[str] = Query(None, description="Filter clients created on or before date (YYYY-MM-DD)"),
    mine: Optional[bool] = Query(None, description="If true, only clients where owner_email matches current user"),
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Export clients as CSV using the same filters as list_clients (no limit; all matching rows)."""
    from datetime import datetime as dt
    from datetime import timedelta
    q = (query or "").strip()
    base_query = db.query(Client)
    if q:
        pattern = f"%{q}%"
        base_query = base_query.filter(Client.business_name.ilike(pattern))
    if stage is not None and stage.strip():
        base_query = base_query.filter(Client.stage == stage.strip())
    if mine:
        user_email = (user_data.get("idinfo") or {}).get("email")
        if user_email:
            base_query = base_query.filter(Client.owner_email == user_email)
    if created_after:
        try:
            start = dt.strptime(created_after, "%Y-%m-%d")
            base_query = base_query.filter(Client.created_at >= start)
        except ValueError:
            pass
    if created_before:
        try:
            end = dt.strptime(created_before, "%Y-%m-%d")
            end_inclusive = end + timedelta(days=1)
            base_query = base_query.filter(Client.created_at < end_inclusive)
        except ValueError:
            pass
    clients = base_query.order_by(Client.business_name.asc()).all()
    rows_data = [ClientResponse.model_validate(c).model_dump(mode="json") for c in clients]
    if not rows_data:
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["id", "business_name", "external_business_id", "primary_contact_email", "gdrive_folder_url", "stage", "stage_changed_at", "owner_email", "created_at", "updated_at"])
        csv_content = buf.getvalue()
    else:
        keys = list(rows_data[0].keys())
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(keys)
        for r in rows_data:
            w.writerow([r.get(k) for k in keys])
        csv_content = buf.getvalue()
    return Response(
        content=csv_content.encode("utf-8-sig"),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=clients_export.csv"},
    )


@app.get("/api/clients/{client_id}", response_model=ClientResponse)
def get_client(
    client_id: int,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Get a single client by id"""
    logging.info(f"Fetching client {client_id}")
    client = db.query(Client).filter(Client.id == client_id).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    resp = ClientResponse.model_validate(client)
    if getattr(client, "referred_by_client_id", None):
        advocate = db.query(Client).filter(Client.id == client.referred_by_client_id).first()
        if advocate:
            resp = resp.model_copy(update={"referred_by_advocate_name": advocate.business_name})
    return resp


@app.patch("/api/clients/{client_id}", response_model=ClientResponse)
def update_client(
    client_id: int,
    client_update: ClientUpdate,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Update client details"""
    logging.info(f"Updating client {client_id}")
    db_client = db.query(Client).filter(Client.id == client_id).first()
    if not db_client:
        raise HTTPException(status_code=404, detail="Client not found")

    if client_update.business_name is not None:
        db_client.business_name = client_update.business_name
    if client_update.external_business_id is not None:
        db_client.external_business_id = client_update.external_business_id
    if client_update.primary_contact_email is not None:
        db_client.primary_contact_email = client_update.primary_contact_email
    if client_update.gdrive_folder_url is not None:
        db_client.gdrive_folder_url = client_update.gdrive_folder_url
    if client_update.owner_email is not None:
        db_client.owner_email = client_update.owner_email
    update_payload = client_update.model_dump(exclude_unset=True)
    if "referred_by_client_id" in update_payload:
        db_client.referred_by_client_id = client_update.referred_by_client_id
    if "referred_by_business_name" in update_payload:
        db_client.referred_by_business_name = client_update.referred_by_business_name
    if "referred_by_active" in update_payload:
        db_client.referred_by_active = 1 if client_update.referred_by_active else 0
    if "advocacy_meeting_date" in update_payload:
        val = client_update.advocacy_meeting_date
        if val and str(val).strip():
            try:
                db_client.advocacy_meeting_date = date.fromisoformat(str(val).strip()[:10])
            except ValueError:
                db_client.advocacy_meeting_date = None
        else:
            db_client.advocacy_meeting_date = None
    if "advocacy_meeting_time" in update_payload:
        db_client.advocacy_meeting_time = (client_update.advocacy_meeting_time or "").strip() or None
    if "advocacy_meeting_completed" in update_payload:
        db_client.advocacy_meeting_completed = 1 if client_update.advocacy_meeting_completed else 0
    if client_update.stage is not None and client_update.stage != db_client.stage:
        db_client.stage = client_update.stage
        db_client.stage_changed_at = datetime.utcnow()

    db.commit()
    db.refresh(db_client)
    logging.info(f"Client {client_id} updated")
    resp = ClientResponse.model_validate(db_client)
    if getattr(db_client, "referred_by_client_id", None):
        advocate = db.query(Client).filter(Client.id == db_client.referred_by_client_id).first()
        if advocate:
            resp = resp.model_copy(update={"referred_by_advocate_name": advocate.business_name})
    return resp


@app.get("/api/clients/{client_id}/referrals", response_model=List[ClientReferralResponse])
def list_client_referrals(
    client_id: int,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """List all advocate/referral entries for this client (the lead)."""
    client = db.query(Client).filter(Client.id == client_id).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    refs = db.query(ClientReferral).filter(ClientReferral.client_id == client_id).order_by(ClientReferral.id).all()
    out = []
    for r in refs:
        resp = ClientReferralResponse.model_validate(r)
        if r.advocate_client_id:
            advocate = db.query(Client).filter(Client.id == r.advocate_client_id).first()
            if advocate:
                resp = resp.model_copy(update={"advocate_display_name": advocate.business_name})
        out.append(resp)
    return out


@app.post("/api/clients/{client_id}/referrals", response_model=ClientReferralResponse)
def create_client_referral(
    client_id: int,
    body: ClientReferralCreate,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Add an advocate/referral entry for this client."""
    client = db.query(Client).filter(Client.id == client_id).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    ref = ClientReferral(
        client_id=client_id,
        advocate_client_id=body.advocate_client_id,
        advocate_business_name=(body.advocate_business_name or "").strip() or None,
        active=1 if (body.active if body.active is not None else True) else 0,
    )
    db.add(ref)
    db.commit()
    db.refresh(ref)
    resp = ClientReferralResponse.model_validate(ref)
    if ref.advocate_client_id:
        advocate = db.query(Client).filter(Client.id == ref.advocate_client_id).first()
        if advocate:
            resp = resp.model_copy(update={"advocate_display_name": advocate.business_name})
    return resp


@app.patch("/api/client-referrals/{referral_id}", response_model=ClientReferralResponse)
def update_client_referral(
    referral_id: int,
    body: ClientReferralUpdate,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Update an advocate/referral entry."""
    ref = db.query(ClientReferral).filter(ClientReferral.id == referral_id).first()
    if not ref:
        raise HTTPException(status_code=404, detail="Referral not found")
    payload = body.model_dump(exclude_unset=True)
    if "advocate_business_name" in payload:
        ref.advocate_business_name = (payload["advocate_business_name"] or "").strip() or None
    if "advocate_client_id" in payload:
        ref.advocate_client_id = payload["advocate_client_id"]
    if "active" in payload:
        ref.active = 1 if payload["active"] else 0
    db.commit()
    db.refresh(ref)
    resp = ClientReferralResponse.model_validate(ref)
    if ref.advocate_client_id:
        advocate = db.query(Client).filter(Client.id == ref.advocate_client_id).first()
        if advocate:
            resp = resp.model_copy(update={"advocate_display_name": advocate.business_name})
    return resp


@app.delete("/api/client-referrals/{referral_id}", response_model=dict)
def delete_client_referral(
    referral_id: int,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Delete an advocate/referral entry."""
    ref = db.query(ClientReferral).filter(ClientReferral.id == referral_id).first()
    if not ref:
        raise HTTPException(status_code=404, detail="Referral not found")
    db.delete(ref)
    db.commit()
    return {"ok": True}


@app.delete("/api/clients/{client_id}", response_model=dict)
def delete_client(
    client_id: int,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """
    Delete a client and all dependent CRM records.

    This removes:
    - Offer activities for this client's offers (and any activities directly linked by client_id)
    - Offers linked to this client
    - Client status notes linked by client_id
    - Tasks linked to this client, plus their TaskHistory entries
    Finally, deletes the Client row itself.
    """
    logging.info(f"Deleting client {client_id} and dependent records")

    client = db.query(Client).filter(Client.id == client_id).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")

    # 1) Offers and offer activities
    offer_ids = [
        o.id
        for o in db.query(Offer.id)
        .filter(Offer.client_id == client_id)
        .all()
    ]
    if offer_ids:
        db.query(OfferActivity).filter(OfferActivity.offer_id.in_(offer_ids)).delete(
            synchronize_session=False
        )
    # Also remove any activities that reference this client directly
    db.query(OfferActivity).filter(OfferActivity.client_id == client_id).delete(
        synchronize_session=False
    )
    db.query(Offer).filter(Offer.client_id == client_id).delete(
        synchronize_session=False
    )

    # 2) Tasks and task history
    task_ids = [
        t.id
        for t in db.query(Task.id)
        .filter(Task.client_id == client_id)
        .all()
    ]
    if task_ids:
        db.query(TaskHistory).filter(TaskHistory.task_id.in_(task_ids)).delete(
            synchronize_session=False
        )
        db.query(Task).filter(Task.id.in_(task_ids)).delete(
            synchronize_session=False
        )

    # 3) Client status notes linked by client_id
    db.query(ClientStatusNote).filter(ClientStatusNote.client_id == client_id).delete(
        synchronize_session=False
    )

    # 4) Client referrals (advocate links)
    db.query(ClientReferral).filter(ClientReferral.client_id == client_id).delete(
        synchronize_session=False
    )
    db.query(ClientReferral).filter(ClientReferral.advocate_client_id == client_id).update(
        {ClientReferral.advocate_client_id: None}, synchronize_session=False
    )

    # 5) Finally, delete the client itself
    db.delete(client)
    db.commit()

    logging.info(f"Client {client_id} and dependent records deleted")
    return {"status": "success", "message": "Client and related data deleted"}


@app.post("/api/clients/search", response_model=List[ClientResponse])
def search_clients(
    search: ClientSearchRequest,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """LEGACY: Search clients by business name substring. Prefer GET /api/clients?query=... for new code."""
    logging.info(f"Searching clients with query: {search.query}")
    q = search.query.strip()
    if not q:
        return []
    pattern = f"%{q}%"
    clients = (
        db.query(Client)
        .filter(Client.business_name.ilike(pattern))
        .order_by(Client.business_name.asc())
        .all()
    )
    return clients


@app.get("/api/search")
def global_search(
    q: str = Query(..., min_length=1),
    limit: int = Query(8, ge=1, le=20),
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Global search for command palette: returns clients (by business_name) and offers (by business_name or identifier)."""
    q_clean = (q or "").strip()
    if not q_clean:
        return {"clients": [], "offers": []}
    pattern = f"%{q_clean}%"
    clients = (
        db.query(Client)
        .filter(Client.business_name.ilike(pattern))
        .order_by(Client.business_name.asc())
        .limit(limit)
        .all()
    )
    offers = (
        db.query(Offer)
        .filter(
            (Offer.business_name.ilike(pattern)) | (Offer.identifier.ilike(pattern))
        )
        .order_by(Offer.created_at.desc())
        .limit(limit)
        .all()
    )
    client_list = [ClientResponse.model_validate(c).model_dump(mode="json") for c in clients]
    offer_list = [_offer_to_response(db, o).model_dump(mode="json") for o in offers]
    return {"clients": client_list, "offers": offer_list}


@app.patch("/api/client-bulk-update", response_model=List[ClientResponse])
def bulk_update_clients(
    body: ClientBulkUpdateRequest,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Bulk update clients: set owner_email and/or stage for the given client IDs. For stage changes, history is recorded."""
    logging.info(
        "[CRM] bulk_update_clients called with body=%s",
        body.model_dump(mode="json"),
    )
    if not body.client_ids:
        logging.info("[CRM] bulk_update_clients: empty client_ids, returning []")
        return []

    user_email = (user_data.get("idinfo") or {}).get("email") or ""

    # Coerce all client_ids to integers and fail fast with a clear error
    try:
        client_ids = [int(raw_id) for raw_id in body.client_ids]
    except (TypeError, ValueError) as exc:
        logging.error(
            "[CRM] bulk_update_clients: failed to coerce client_ids=%r (types=%r): %s",
            body.client_ids,
            [type(x) for x in body.client_ids],
            exc,
        )
        raise HTTPException(
            status_code=422,
            detail="Invalid client_ids: all client_ids must be integers.",
        )
    updated: List[Client] = []
    any_changes = False

    for client_id in client_ids:
        client = db.query(Client).filter(Client.id == client_id).first()
        if not client:
            continue

        if body.owner_email is not None:
            client.owner_email = body.owner_email.strip() or None
            any_changes = True

        if body.stage is not None:
            update_client_stage_with_history(
                db=db,
                client_id=client_id,
                new_stage=body.stage,
                user_email=user_email,
                commit=False,
            )
            any_changes = True

        updated.append(client)

    logging.info(
        "[CRM] bulk_update_clients: any_changes=%s, client_ids=%s, stage=%s, owner_email=%s, user_email=%s",
        any_changes,
        client_ids,
        body.stage,
        body.owner_email,
        user_email,
    )

    if any_changes:
        db.commit()
        for client in updated:
            db.refresh(client)

    return [
        ClientResponse.model_validate(c).model_dump(mode="json") for c in updated
    ]


@app.patch("/api/clients/{client_id}/stage", response_model=ClientResponse)
def update_client_stage(
    client_id: int,
    stage_update: ClientStageUpdateRequest,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Update a client's pipeline stage."""
    user_info = user_data["idinfo"]
    user_email = user_info.get("email")

    logging.info(f"Updating client {client_id} stage to {stage_update.stage}")
    return update_client_stage_with_history(
        db=db,
        client_id=client_id,
        new_stage=stage_update.stage,
        user_email=user_email,
    )


@app.get("/api/clients/{client_id}/activities", response_model=List[OfferActivityResponse])
def list_client_activities(
    client_id: int,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """List offer activities for all offers belonging to this client, newest first."""
    # Verify client exists
    client = db.query(Client).filter(Client.id == client_id).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    activities = (
        db.query(OfferActivity)
        .join(Offer, OfferActivity.offer_id == Offer.id)
        .filter(Offer.client_id == client_id)
        .order_by(OfferActivity.created_at.desc())
        .all()
    )
    return [OfferActivityResponse.model_validate(a) for a in activities]


@app.get("/api/clients/{client_id}/notes", response_model=List[ClientStatusNoteResponse])
def get_client_notes_by_id(
    client_id: int,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Get all notes for a client by id"""
    logging.info(f"Fetching notes for client_id {client_id}")
    notes = (
        db.query(ClientStatusNote)
        .filter(ClientStatusNote.client_id == client_id)
        .order_by(ClientStatusNote.created_at.desc())
        .all()
    )
    return notes


@app.post("/api/clients/{client_id}/notes", response_model=ClientStatusNoteResponse)
def create_client_note_for_id(
    client_id: int,
    note: ClientStatusNoteCreate,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Create a client note for a given client id"""
    user_info = user_data["idinfo"]
    user_email = user_info.get("email")

    db_client = db.query(Client).filter(Client.id == client_id).first()
    if not db_client:
        raise HTTPException(status_code=404, detail="Client not found")

    logging.info(f"Creating note for client_id {client_id}")

    db_note = ClientStatusNote(
        business_name=db_client.business_name,
        client_id=client_id,
        note=note.note,
        user_email=user_email,
        note_type=note.note_type or "general",
        related_task_id=note.related_task_id,
        related_offer_id=note.related_offer_id,
    )
    db.add(db_note)
    db.commit()
    db.refresh(db_note)
    return db_note


def _parse_activity_metadata(meta_raw):  # noqa: ANN001
    """Parse activity metadata from DB (may be JSON string)."""
    if meta_raw is None:
        return None
    if isinstance(meta_raw, dict):
        return meta_raw
    if isinstance(meta_raw, str):
        try:
            return json.loads(meta_raw)
        except (TypeError, json.JSONDecodeError):
            return None
    return None


def _activity_type_to_source_prefix(activity_type: str) -> Optional[str]:
    """Map activity_type to display prefix: Base 2, DMA, or Comparison."""
    if not activity_type:
        return None
    at = (activity_type or "").strip().lower()
    if at == "base2_review":
        return "Base 2"
    if at in ("dma_review_generated", "dma_email_sent"):
        return "DMA"
    if at == "comparison":
        return "Comparison"
    return None


def _get_offer_source_prefix_from_activities(db: Session, offer: Offer) -> Optional[str]:
    """Return the source prefix (Base 2, DMA, Comparison) from the offer's most recent relevant activity."""
    activities = (
        db.query(OfferActivity)
        .filter(OfferActivity.offer_id == offer.id)
        .order_by(OfferActivity.created_at.desc())
        .limit(20)
        .all()
    )
    for a in activities:
        prefix = _activity_type_to_source_prefix(getattr(a, "activity_type", None))
        if prefix:
            return prefix
    return None


def _derived_offer_fields_from_activities(db: Session, offer: Offer) -> dict:
    """
    When an offer has no utility_type/identifier set, derive from its activities' metadata
    so the list and detail pages show data (e.g. Gas, NMI) from Base 2 / comparison runs.
    Also sets source_prefix (Base 2, DMA, Comparison) for utility_display.
    """
    out = {}
    if offer.utility_type or offer.utility_type_identifier or offer.identifier:
        return out
    activities = (
        db.query(OfferActivity)
        .filter(OfferActivity.offer_id == offer.id)
        .order_by(OfferActivity.created_at.desc())
        .limit(50)
        .all()
    )
    utility_type = None
    utility_type_identifier = None
    identifier = None
    source_prefix = None
    for a in activities:
        meta = _parse_activity_metadata(getattr(a, "metadata_", None))
        if not meta:
            continue
        if utility_type is None and meta.get("utility_type"):
            utility_type = (meta.get("utility_type") or "").strip()
            if source_prefix is None:
                source_prefix = _activity_type_to_source_prefix(getattr(a, "activity_type", None))
        if utility_type is None and meta.get("comparison_type"):
            utility_type = (meta.get("comparison_type") or "").strip()
            if source_prefix is None:
                source_prefix = _activity_type_to_source_prefix(getattr(a, "activity_type", None))
        if utility_type_identifier is None and meta.get("utility_type_identifier"):
            utility_type_identifier = (meta.get("utility_type_identifier") or "").strip()
        if identifier is None:
            id_val = (
                meta.get("nmi")
                or meta.get("mrin")
                or meta.get("identifier")
                or meta.get("account_number")
                or meta.get("account_name")
            )
            if id_val:
                identifier = str(id_val).strip()
        if utility_type and identifier and utility_type_identifier and source_prefix is not None:
            break
    if utility_type:
        raw = (utility_type or "").lower()
        if not utility_type_identifier:
            utility_type_identifier = (
                "Electricity" if raw == "electricity"
                else "Gas" if raw == "gas"
                else "Waste" if raw == "waste"
                else "Oil" if raw == "oil"
                else "Cleaning" if raw == "cleaning"
                else "DMA" if raw == "dma"
                else utility_type
            )
        out["utility_type"] = utility_type
        out["utility_type_identifier"] = utility_type_identifier
        if source_prefix is None:
            source_prefix = _activity_type_to_source_prefix(
                activities[0].activity_type if activities else None
            )
        out["source_prefix"] = source_prefix
    if identifier:
        out["identifier"] = identifier
    return out


def _offer_to_response(db: Session, offer: Offer) -> OfferResponse:
    """Build OfferResponse with derived is_existing_client from linked client stage.
    When offer has no utility/identifier, derive from activities so list and detail show data.
    Sets utility_display as 'Base 2 Gas' / 'DMA Electricity' / 'Comparison Gas' when source is known."""
    is_existing = False
    if offer.client_id:
        client = db.query(Client).filter(Client.id == offer.client_id).first()
        if client and (client.stage or "").strip():
            stage_val = (client.stage or "").strip().lower()
            if stage_val in (s.value for s in POST_WIN_STAGES):
                is_existing = True
    base = OfferResponse.model_validate(offer).model_copy(
        update={"is_existing_client": is_existing}
    )
    utility_display = base.utility_type_identifier or base.utility_type
    if not offer.utility_type and not offer.utility_type_identifier and not offer.identifier:
        derived = _derived_offer_fields_from_activities(db, offer)
        if derived:
            label = derived.get("utility_type_identifier") or derived.get("utility_type")
            prefix = derived.get("source_prefix")
            if prefix and label:
                utility_display = f"{prefix} {label}"
            elif label:
                utility_display = label
            base = base.model_copy(update={
                "utility_type": derived.get("utility_type") or base.utility_type,
                "utility_type_identifier": derived.get("utility_type_identifier") or base.utility_type_identifier,
                "identifier": derived.get("identifier") or base.identifier,
            })
    else:
        # Offer has utility data; still add source prefix from activities if present
        prefix = _get_offer_source_prefix_from_activities(db, offer)
        if prefix and utility_display:
            utility_display = f"{prefix} {utility_display}"
    if utility_display:
        base = base.model_copy(update={"utility_display": utility_display})
    return base


@app.post("/api/offers", response_model=OfferResponse)
def create_offer(
    offer: OfferCreate,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Create a new offer (quote request record). Valid for any client stage (including Won/Existing client)."""
    user_info = user_data["idinfo"]
    user_email = user_info.get("email")

    db_offer = Offer(
        client_id=offer.client_id,
        business_name=offer.business_name,
        utility_type=offer.utility_type,
        utility_type_identifier=offer.utility_type_identifier,
        identifier=offer.identifier,
        status=(offer.status or OfferStatus.REQUESTED).value
        if isinstance(offer.status, OfferStatus)
        else (offer.status or OfferStatus.REQUESTED.value),
        pipeline_stage=offer.pipeline_stage.value
        if isinstance(getattr(offer, "pipeline_stage", None), OfferPipelineStageSchema)
        else getattr(offer, "pipeline_stage", None),
        estimated_value=offer.estimated_value,
        created_by=user_email,
        external_record_id=offer.external_record_id,
        document_link=offer.document_link,
    )
    db.add(db_offer)
    db.commit()
    db.refresh(db_offer)
    logging.info(f"Offer created with id {db_offer.id}")
    return _offer_to_response(db, db_offer)


@app.get("/api/offers")
def list_offers(
    client_id: Optional[int] = Query(None, description="Filter by client id"),
    status: Optional[str] = Query(None, description="Filter by status"),
    utility: Optional[str] = Query(None, description="Filter by utility type or label (substring match)"),
    identifier: Optional[str] = Query(None, description="Filter by identifier (substring match, e.g. NMI/MIRN)"),
    created_after: Optional[str] = Query(None, description="Filter offers created on or after date (YYYY-MM-DD)"),
    created_before: Optional[str] = Query(None, description="Filter offers created on or before date (YYYY-MM-DD)"),
    mine: Optional[bool] = Query(None, description="If true, only offers whose linked client has owner_email = current user"),
    limit: Optional[int] = Query(None, description="Max number of offers to return (enables paginated response with total)"),
    offset: Optional[int] = Query(None, description="Number of offers to skip (use with limit)"),
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """List offers, optionally filtered by client, status, utility, identifier, date range, or mine (by client owner). When limit/offset set, returns { items, total }."""
    from datetime import datetime as dt
    from datetime import timedelta
    logging.info("Listing offers")
    query = db.query(Offer)
    if client_id is not None:
        query = query.filter(Offer.client_id == client_id)
    if mine:
        user_email = (user_data.get("idinfo") or {}).get("email")
        if user_email:
            query = query.join(Client, Offer.client_id == Client.id).filter(Client.owner_email == user_email)
        else:
            query = query.filter(Offer.id == -1)
    if status is not None:
        query = query.filter(Offer.status == status)
    if utility and utility.strip():
        term = f"%{utility.strip()}%"
        query = query.filter(
            or_(
                Offer.utility_type.ilike(term),
                Offer.utility_type_identifier.ilike(term),
            )
        )
    if identifier and identifier.strip():
        query = query.filter(Offer.identifier.ilike(f"%{identifier.strip()}%"))
    if created_after:
        try:
            start = dt.strptime(created_after, "%Y-%m-%d")
            query = query.filter(Offer.created_at >= start)
        except ValueError:
            pass
    if created_before:
        try:
            end = dt.strptime(created_before, "%Y-%m-%d")
            end_inclusive = end + timedelta(days=1)
            query = query.filter(Offer.created_at < end_inclusive)
        except ValueError:
            pass
    ordered = query.order_by(Offer.created_at.desc())
    if limit is not None or offset is not None:
        total = ordered.count()
        off = offset or 0
        lim = limit or 20
        offers = ordered.offset(off).limit(lim).all()
        items = [_offer_to_response(db, o) for o in offers]
        return JSONResponse(content={"items": [r.model_dump(mode="json") for r in items], "total": total})
    offers = ordered.all()
    return [_offer_to_response(db, o) for o in offers]


@app.get("/api/offers/export")
def export_offers_csv(
    client_id: Optional[int] = Query(None, description="Filter by client id"),
    status: Optional[str] = Query(None, description="Filter by status"),
    utility: Optional[str] = Query(None, description="Filter by utility type or label (substring match)"),
    identifier: Optional[str] = Query(None, description="Filter by identifier (substring match, e.g. NMI/MIRN)"),
    created_after: Optional[str] = Query(None, description="Filter offers created on or after date (YYYY-MM-DD)"),
    created_before: Optional[str] = Query(None, description="Filter offers created on or before date (YYYY-MM-DD)"),
    mine: Optional[bool] = Query(None, description="If true, only offers whose linked client has owner_email = current user"),
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Export offers as CSV using the same filters as list_offers (no limit; all matching rows)."""
    from datetime import datetime as dt
    from datetime import timedelta
    query = db.query(Offer)
    if client_id is not None:
        query = query.filter(Offer.client_id == client_id)
    if mine:
        user_email = (user_data.get("idinfo") or {}).get("email")
        if user_email:
            query = query.join(Client, Offer.client_id == Client.id).filter(Client.owner_email == user_email)
        else:
            query = query.filter(Offer.id == -1)
    if status is not None:
        query = query.filter(Offer.status == status)
    if utility and utility.strip():
        term = f"%{utility.strip()}%"
        query = query.filter(
            or_(
                Offer.utility_type.ilike(term),
                Offer.utility_type_identifier.ilike(term),
            )
        )
    if identifier and identifier.strip():
        query = query.filter(Offer.identifier.ilike(f"%{identifier.strip()}%"))
    if created_after:
        try:
            start = dt.strptime(created_after, "%Y-%m-%d")
            query = query.filter(Offer.created_at >= start)
        except ValueError:
            pass
    if created_before:
        try:
            end = dt.strptime(created_before, "%Y-%m-%d")
            end_inclusive = end + timedelta(days=1)
            query = query.filter(Offer.created_at < end_inclusive)
        except ValueError:
            pass
    offers = query.order_by(Offer.created_at.desc()).all()
    rows_data = [_offer_to_response(db, o).model_dump(mode="json") for o in offers]
    if not rows_data:
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["id", "client_id", "business_name", "utility_type", "utility_type_identifier", "identifier", "status", "estimated_value", "created_by", "external_record_id", "document_link", "created_at", "updated_at", "is_existing_client"])
        csv_content = buf.getvalue()
    else:
        keys = list(rows_data[0].keys())
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(keys)
        for r in rows_data:
            w.writerow([r.get(k) for k in keys])
        csv_content = buf.getvalue()
    return Response(
        content=csv_content.encode("utf-8-sig"),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=offers_export.csv"},
    )


@app.get("/api/offers/{offer_id}", response_model=OfferResponse)
def get_offer(
    offer_id: int,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Get a single offer."""
    logging.info(f"Fetching offer {offer_id}")
    offer = db.query(Offer).filter(Offer.id == offer_id).first()
    if not offer:
        raise HTTPException(status_code=404, detail="Offer not found")
    return _offer_to_response(db, offer)


@app.get("/api/offers/{offer_id}/activities", response_model=List[OfferActivityResponse])
def list_offer_activities(
    offer_id: int,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """List activities for an offer, newest first."""
    offer = db.query(Offer).filter(Offer.id == offer_id).first()
    if not offer:
        raise HTTPException(status_code=404, detail="Offer not found")
    activities = (
        db.query(OfferActivity)
        .filter(OfferActivity.offer_id == offer_id)
        .order_by(OfferActivity.created_at.desc())
        .all()
    )
    return [OfferActivityResponse.model_validate(a) for a in activities]


@app.post("/api/offers/{offer_id}/activities", response_model=OfferActivityResponse)
def post_offer_activity(
    offer_id: int,
    body: OfferActivityCreate,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Create an activity for an offer (e.g. discrepancy_email_sent from UI, or from other flows)."""
    offer = db.query(Offer).filter(Offer.id == offer_id).first()
    if not offer:
        raise HTTPException(status_code=404, detail="Offer not found")
    client = None
    if offer.client_id:
        client = db.query(Client).filter(Client.id == offer.client_id).first()
    created_by = body.created_by or (user_data.get("email") if user_data else None)
    # Normalise/augment metadata so utility context is always present where possible.
    metadata = dict(body.metadata or {}) if body.metadata is not None else {}
    if "utility_type" not in metadata and getattr(offer, "utility_type", None):
        metadata["utility_type"] = offer.utility_type
    if "utility_type_identifier" not in metadata and getattr(offer, "utility_type_identifier", None):
        metadata["utility_type_identifier"] = offer.utility_type_identifier
    activity = create_offer_activity(
        db,
        offer=offer,
        client=client,
        activity_type=body.activity_type,
        document_link=body.document_link,
        external_id=body.external_id,
        metadata=metadata,
        created_by=created_by,
    )
    return OfferActivityResponse.model_validate(activity)


@app.patch("/api/offers/{offer_id}", response_model=OfferResponse)
def update_offer(
    offer_id: int,
    offer_update: OfferUpdate,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Update an offer"""
    logging.info(f"Updating offer {offer_id}")
    db_offer = db.query(Offer).filter(Offer.id == offer_id).first()
    if not db_offer:
        raise HTTPException(status_code=404, detail="Offer not found")

    if offer_update.client_id is not None:
        db_offer.client_id = offer_update.client_id
    if offer_update.business_name is not None:
        db_offer.business_name = offer_update.business_name
    if offer_update.utility_type is not None:
        db_offer.utility_type = offer_update.utility_type
    if offer_update.utility_type_identifier is not None:
        db_offer.utility_type_identifier = offer_update.utility_type_identifier
    if offer_update.identifier is not None:
        db_offer.identifier = offer_update.identifier
    if offer_update.status is not None:
        db_offer.status = (
            offer_update.status.value
            if isinstance(offer_update.status, OfferStatus)
            else str(offer_update.status)
        )
    if offer_update.estimated_value is not None:
        db_offer.estimated_value = offer_update.estimated_value
    if offer_update.annual_savings is not None:
        db_offer.annual_savings = offer_update.annual_savings
    if offer_update.current_cost is not None:
        db_offer.current_cost = offer_update.current_cost
    if offer_update.new_cost is not None:
        db_offer.new_cost = offer_update.new_cost
    if offer_update.external_record_id is not None:
        db_offer.external_record_id = offer_update.external_record_id
    if offer_update.document_link is not None:
        db_offer.document_link = offer_update.document_link
    if offer_update.pipeline_stage is not None:
        db_offer.pipeline_stage = (
            offer_update.pipeline_stage.value
            if isinstance(offer_update.pipeline_stage, OfferPipelineStageSchema)
            else str(offer_update.pipeline_stage)
        )

    if offer_update.status is not None:
        sync_strategy_status_from_offer(db, db_offer)
    if offer_update.annual_savings is not None or offer_update.estimated_value is not None:
        sync_strategy_status_from_offer(db, db_offer)

    db.commit()
    db.refresh(db_offer)
    return _offer_to_response(db, db_offer)


@app.patch("/api/offers/{offer_id}/status", response_model=OfferResponse)
def update_offer_status(
    offer_id: int,
    status_update: OfferStatusUpdateRequest,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Update only the status of an offer and optionally move client stage."""
    logging.info(f"Updating offer {offer_id} status to {status_update.status}")
    updated = update_offer_status_and_propagate_client_stage(
        db=db,
        offer_id=offer_id,
        new_status=status_update.status,
    )
    return _offer_to_response(db, updated)


@app.patch("/api/offers/{offer_id}/pipeline-stage", response_model=OfferResponse)
def update_offer_pipeline_stage(
    offer_id: int,
    body: OfferPipelineStageUpdateRequest,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """
    Update the detailed pipeline stage for an offer.

    For terminal pipeline stages (contract_accepted / lost) we delegate to status
    propagation so that client lifecycle remains consistent.
    """
    logging.info(
        "Updating offer %s pipeline_stage to %s", offer_id, body.pipeline_stage
    )

    stage = body.pipeline_stage
    if stage in (
        OfferPipelineStageSchema.CONTRACT_ACCEPTED,
        OfferPipelineStageSchema.LOST,
    ):
        status = (
            OfferStatus.ACCEPTED
            if stage == OfferPipelineStageSchema.CONTRACT_ACCEPTED
            else OfferStatus.LOST
        )
        updated = update_offer_status_and_propagate_client_stage(
            db=db,
            offer_id=offer_id,
            new_status=status,
        )
        return _offer_to_response(db, updated)

    offer = db.query(Offer).filter(Offer.id == offer_id).first()
    if not offer:
        raise HTTPException(status_code=404, detail="Offer not found")

    offer.pipeline_stage = (
        stage.value if isinstance(stage, OfferPipelineStageSchema) else str(stage)
    )
    db.commit()
    db.refresh(offer)
    return _offer_to_response(db, offer)


@app.delete("/api/offers/{offer_id}", response_model=dict)
def delete_offer(
    offer_id: int,
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """
    Delete a single offer and its dependent records.

    This removes:
    - OfferActivity rows for this offer
    - ClientStatusNote rows linked via related_offer_id for this offer
    Finally, deletes the Offer row.
    """
    logging.info(f"Deleting offer {offer_id} and dependent records")

    offer = db.query(Offer).filter(Offer.id == offer_id).first()
    if not offer:
        raise HTTPException(status_code=404, detail="Offer not found")

    # Delete activities for this offer
    db.query(OfferActivity).filter(OfferActivity.offer_id == offer_id).delete(
        synchronize_session=False
    )

    # Delete notes that are explicitly linked to this offer
    db.query(ClientStatusNote).filter(
        ClientStatusNote.related_offer_id == offer_id
    ).delete(synchronize_session=False)

    # Delete the offer itself
    db.delete(offer)
    db.commit()

    logging.info(f"Offer {offer_id} and dependent records deleted")
    return {"status": "success", "message": "Offer and related data deleted"}


@app.get("/api/reports/clients/offer-counts")
def clients_offer_counts(
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Return map of client_id (as string) to offer count for pipeline cards."""
    rows = (
        db.query(Offer.client_id, func.count(Offer.id))
        .filter(Offer.client_id.isnot(None))
        .group_by(Offer.client_id)
        .all()
    )
    return {str(client_id): count for client_id, count in rows}


@app.get("/api/reports/pipeline/summary")
def pipeline_summary(
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Simple summary of clients by stage for CRM dashboard widgets."""
    logging.info("Generating pipeline summary")

    total_clients = db.query(func.count(Client.id)).scalar() or 0

    rows = (
        db.query(Client.stage, func.count(Client.id))
        .group_by(Client.stage)
        .all()
    )
    raw_counts = {stage or ClientStage.LEAD.value: count or 0 for stage, count in rows}

    by_stage = [
        {"stage": stage.value, "count": raw_counts.get(stage.value, 0)}
        for stage in ClientStage
    ]

    won_count = raw_counts.get(ClientStage.WON.value, 0)
    lost_count = raw_counts.get(ClientStage.LOST.value, 0)

    return {
        "total_clients": total_clients,
        "by_stage": by_stage,
        "won_count": won_count,
        "lost_count": lost_count,
    }


@app.get("/api/reports/pipeline/won-lost-by-period")
def won_lost_by_period(
    period: Optional[str] = Query("month", description="Grouping period: 'month' (default) or 'year'"),
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Read-only report: count of clients that moved to won or lost, grouped by period (month or year). Uses stage_changed_at (fallback updated_at)."""
    from collections import defaultdict
    period_format = "%Y-%m" if (period or "month").lower() == "month" else "%Y"
    period_expr = func.strftime(
        period_format,
        func.coalesce(Client.stage_changed_at, Client.updated_at),
    )
    rows = (
        db.query(period_expr.label("period"), Client.stage, func.count(Client.id))
        .filter(Client.stage.in_([ClientStage.WON.value, ClientStage.LOST.value]))
        .group_by(period_expr, Client.stage)
        .all()
    )
    by_period = defaultdict(lambda: {"won": 0, "lost": 0})
    for p, stage, count in rows:
        if stage == ClientStage.WON.value:
            by_period[p]["won"] = count
        elif stage == ClientStage.LOST.value:
            by_period[p]["lost"] = count
    result = [
        {"period": p, "won": by_period[p]["won"], "lost": by_period[p]["lost"]}
        for p in sorted(by_period.keys(), reverse=True)
    ]
    return result


@app.get("/api/reports/tasks/summary")
def tasks_summary(
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Simple summary of tasks by status and basic due metrics."""
    logging.info("Generating tasks summary")

    total_tasks = db.query(func.count(Task.id)).scalar() or 0

    rows = (
        db.query(Task.status, func.count(Task.id))
        .group_by(Task.status)
        .all()
    )
    by_status = {status or "unknown": count or 0 for status, count in rows}

    now = datetime.utcnow()
    overdue = (
        db.query(func.count(Task.id))
        .filter(
            Task.due_date.isnot(None),
            Task.due_date < now,
            func.lower(Task.status) != "completed",
        )
        .scalar()
        or 0
    )

    # Due today (based on UTC date)
    start_of_day = datetime(now.year, now.month, now.day)
    end_of_day = datetime(now.year, now.month, now.day, 23, 59, 59)
    due_today = (
        db.query(func.count(Task.id))
        .filter(
            Task.due_date.isnot(None),
            Task.due_date >= start_of_day,
            Task.due_date <= end_of_day,
            func.lower(Task.status) != "completed",
        )
        .scalar()
        or 0
    )

    return {
        "total_tasks": total_tasks,
        "by_status": by_status,
        "overdue": overdue,
        "due_today": due_today,
    }


@app.get("/api/reports/activities/list", response_model=List[ActivityReportItem])
def activities_report_list(
    client_id: Optional[int] = Query(None, description="Filter by client id"),
    activity_type: Optional[str] = Query(None, description="Filter by activity type"),
    created_after: Optional[str] = Query(None, description="On or after date (YYYY-MM-DD)"),
    created_before: Optional[str] = Query(None, description="On or before date (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """List offer activities with optional filters, newest first. Includes business_name from offer."""
    from datetime import datetime as dt
    from datetime import timedelta
    logging.info("Listing activities for report")
    query = (
        db.query(OfferActivity, Offer)
        .join(Offer, OfferActivity.offer_id == Offer.id)
    )
    if client_id is not None:
        query = query.filter(Offer.client_id == client_id)
    if activity_type is not None and activity_type.strip():
        query = query.filter(OfferActivity.activity_type == activity_type.strip())
    if created_after:
        try:
            start = dt.strptime(created_after, "%Y-%m-%d")
            query = query.filter(OfferActivity.created_at >= start)
        except ValueError:
            pass
    if created_before:
        try:
            end = dt.strptime(created_before, "%Y-%m-%d")
            end_inclusive = end + timedelta(days=1)
            query = query.filter(OfferActivity.created_at < end_inclusive)
        except ValueError:
            pass
    rows = (
        query.order_by(OfferActivity.created_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )
    out = []
    for act, offer in rows:
        resp = _offer_to_response(db, offer)
        offer_display = (resp.utility_display or "Offer")
        if resp.identifier:
            offer_display = f"{offer_display} {resp.identifier}"
        out.append(
            ActivityReportItem(
                id=act.id,
                offer_id=act.offer_id,
                client_id=act.client_id,
                business_name=offer.business_name,
                activity_type=act.activity_type,
                document_link=act.document_link,
                created_at=act.created_at,
                created_by=act.created_by,
                offer_display=offer_display or None,
            )
        )
    return out


@app.get("/api/reports/activities/summary")
def activities_summary(
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Counts of offer activities by activity_type for dashboard/reports."""
    logging.info("Generating activities summary")
    total = db.query(func.count(OfferActivity.id)).scalar() or 0
    rows = (
        db.query(OfferActivity.activity_type, func.count(OfferActivity.id))
        .group_by(OfferActivity.activity_type)
        .all()
    )
    by_type = {activity_type: count for activity_type, count in rows}
    return {"total": total, "by_type": by_type}


@app.get("/api/reports/offers/summary")
def offers_summary(
    db: Session = Depends(get_db),
    user_data: dict = Depends(get_current_user_with_db),
):
    """Simple summary of offers for CRM dashboard widgets."""
    logging.info("Generating offers summary")

    total_offers = db.query(func.count(Offer.id)).scalar() or 0

    rows = (
        db.query(Offer.status, func.count(Offer.id))
        .group_by(Offer.status)
        .all()
    )
    raw_counts = {status or OfferStatus.REQUESTED.value: count or 0 for status, count in rows}

    by_status = {status.value: raw_counts.get(status.value, 0) for status in OfferStatus}

    accepted = by_status.get(OfferStatus.ACCEPTED.value, 0)
    lost = by_status.get(OfferStatus.LOST.value, 0)

    win_rate = 0.0
    if accepted + lost > 0:
        win_rate = accepted / float(accepted + lost)

    return {
        "total_offers": total_offers,
        "by_status": by_status,
        "accepted": accepted,
        "lost": lost,
        "win_rate": win_rate,
    }