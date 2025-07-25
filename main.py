from fastapi import FastAPI, Depends, HTTPException, Query, UploadFile, File, Form
from fastapi import Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from google.oauth2 import id_token
from google.auth.transport import requests as grequests
from dotenv import load_dotenv
import os
import logging
from typing import Optional

# Adjust this import if your function is in a different location
from tools.business_info import get_business_information
from tools.get_electricity_ci_latest_invoice_information import get_electricity_ci_latest_invoice_information
from tools.get_electricity_sme_latest_invoice_information import get_electricity_sme_latest_invoice_information
from tools.get_gas_latest_invoice_information import get_gas_latest_invoice_information
from tools.get_gas_sme_latest_invoice_information import get_gas_sme_latest_invoice_information
from tools.get_waste_latest_invoice_information import get_waste_latest_invoice_information
from tools.get_oil_invoice_information import get_oil_invoice_information
from tools.supplier_data_request import supplier_data_request
from tools.drive_filing import drive_filing

load_dotenv()
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://acesagentinterface-672026052958.australia-southeast2.run.app",
        "https://acesagentinterfacedev-672026052958.australia-southeast2.run.app",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from fastapi import Header

def verify_google_token(authorization: str = Header(...)):
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    
    token = authorization.split("Bearer ")[1]
    
    try:
        idinfo = id_token.verify_oauth2_token(token, grequests.Request(), GOOGLE_CLIENT_ID)
        logging.info(f"Decoded user info: {idinfo}")
        return idinfo
    except Exception as e:
        logging.error(f"Token verification failed: {str(e)}")
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")


class BusinessInfoRequest(BaseModel):
    business_name: str

@app.post("/api/get-business-info")
def get_business_info(
    request: BusinessInfoRequest,
    user_info: dict = Depends(verify_google_token)
):
    logging.info(f"Received business info request: {request}")
    result = get_business_information(request.business_name)
    if isinstance(result, dict):
        result["user_email"] = user_info.get("email")
    logging.info(f"Returning response to frontend: {result}")
    return result

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

class DataRequest(BaseModel):
    business_name: str
    supplier_name: str
    request_type: str
    details: Optional[str] = None

# --- New Endpoints for ACES ---

@app.post("/api/get-electricity-ci-info")
def get_electricity_ci_info(
    request: ElectricityInvoiceRequest,
    user_info: dict = Depends(verify_google_token)
):
    if not request.business_name or not request.nmi:
        raise HTTPException(status_code=400, detail="business_name and nmi are required")
    
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
    if not request.business_name or not request.nmi:
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
    if not request.business_name or not request.mrin:
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
    if not request.business_name or not request.mrin:
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
    if not request.business_name or not request.account_number:
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

@app.post("/api/drive-filing")
def drive_filing_endpoint(
    business_name: str = Form(...),
    gdrive_url: str = Form(...),
    filing_type: str = Form(...),
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
        filing_type=filing_type
    )
    result["user_email"] = user_info.get("email")
    logging.info(f"Returning drive filing response to frontend: {result}")
    return result

@app.post("/api/data-request")
def data_request(
    request: DataRequest,
    user_info: dict = Depends(verify_google_token)
):
    logging.info(f"Received data request: {request}")
    service_type = request.request_type
    account_identifier = request.details or ""

    # Map service_type to identifier_type
    if service_type in ["electricity_ci", "electricity_sme"]:
        identifier_type = "NMI"
    elif service_type in ["gas_ci", "gas_sme"]:
        identifier_type = "MRIN"
    elif service_type == "waste":
        identifier_type = "account_number"
    else:
        identifier_type = "NMI"  # default fallback

    result = supplier_data_request(
        supplier_name=request.supplier_name,
        business_name=request.business_name,
        service_type=service_type,
        account_identifier=account_identifier,
        identifier_type=identifier_type
    )
    if isinstance(result, dict):
        result["user_email"] = user_info.get("email")
    else:
        result = {"status": "error", "message": result, "user_email": user_info.get("email")}
    logging.info(f"Returning data request response to frontend: {result}")
    return result
