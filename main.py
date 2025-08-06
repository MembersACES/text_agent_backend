from fastapi import FastAPI, Depends, HTTPException, Query, UploadFile, File, Form
from fastapi import Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi import UploadFile, File, Form
from pydantic import BaseModel
from google.oauth2 import id_token
from google.auth.transport import requests as grequests
from dotenv import load_dotenv
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
from typing import Optional
import json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import uuid
import google.auth

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
from tools.send_supplier_signed_agreement import send_supplier_signed_agreement
from tools.document_generation import (
    loa_generation,
    service_fee_agreement_generation,
    expression_of_interest_generation,
    ghg_offer_generation,
    get_available_eoi_types
)

from tools.loa_generation import loa_generation_new

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
        "https://acesagentinterfacedev-672026052958.australia-southeast2.run.app/canva-pitch-deck",
        "https://acesagentinterface-672026052958.australia-southeast2.run.app/canva-pitch-deck",
        "https://acesagentinterfacedev-672026052958.australia-southeast2.run.app/document-lodgement",
        "http://localhost:3000",
        "http://localhost:3000/signed-agreement-lodgement",
        "http://localhost:3000/document-generation",
        "http://localhost:3000/new-client-loa",
        "https://acesagentinterface-672026052958.australia-southeast2.run.app/google-presentations",
        "http://localhost:3000/google-presentations",
         "https://acesagentinterface-672026052958.australia-southeast2.run.app/strategy-generator",
        "http://localhost:3000/strategy-generator",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from fastapi import Header

def verify_google_access_token(authorization: str = Header(...)):
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    
    access_token = authorization.split("Bearer ")[1]
    
    try:
        # For access tokens, we create credentials directly
        credentials = Credentials(token=access_token)
        
        # Test the credentials by making a simple API call
        service = build('drive', 'v3', credentials=credentials)
        service.files().list(pageSize=1).execute()
        
        return {"access_token": access_token}
    except Exception as e:
        logging.error(f"Access token verification failed: {str(e)}")
        raise HTTPException(status_code=401, detail="Invalid access token")

def verify_google_token(authorization: str = Header(...)):
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    
    token = authorization.split("Bearer ")[1]
    
    try:
        idinfo = id_token.verify_oauth2_token(token, grequests.Request(), GOOGLE_CLIENT_ID)
        logging.info(f"Decoded user info: {idinfo}")
        return idinfo
    except Exception as e:
        error_msg = str(e)
        logging.error(f"Token verification failed: {error_msg}")
        
        if "Token expired" in error_msg:
            raise HTTPException(
                status_code=401,
                detail="REAUTHENTICATION_REQUIRED"  # Use a specific code
            )
        
        raise HTTPException(status_code=401, detail="Invalid token")

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

class RobotDataRequest(BaseModel):
    robot_number: str
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

class SignedAgreementRequest(BaseModel):
    business_name: str
    contract_type: str
    agreement_type: str = "contract"

@app.post("/api/signed-agreement-lodgement")
async def signed_agreement_lodgement(
    business_name: str = Form(...),
    contract_type: str = Form(...),
    agreement_type: str = Form("contract"),
    file: UploadFile = File(...),
    user_info: dict = Depends(verify_google_token)
):
    """
    Handle signed agreement lodgement with file upload
    """
    logging.info(f"Received signed agreement request: business_name={business_name}, contract_type={contract_type}, agreement_type={agreement_type}, filename={file.filename}")
    
    # Validate file type
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are accepted")
    
    # Create a temporary file to save the uploaded content
    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
        try:
            # Read and write file content
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name
            
            # Call the existing tool function
            result = send_supplier_signed_agreement(
                file_path=temp_file_path,
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
                "agreement_type": agreement_type
            }
            
            logging.info(f"Returning signed agreement response to frontend: {response}")
            return response
            
        except Exception as e:
            logging.error(f"Error processing signed agreement: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error processing agreement: {str(e)}")
        
        finally:
            # Clean up temporary file
            try:
                os.unlink(temp_file_path)
            except Exception as e:
                logging.warning(f"Could not delete temporary file: {str(e)}")

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

@app.post("/api/generate-loa")
def generate_loa_endpoint(
    request: DocumentGenerationRequest,
    user_info: dict = Depends(verify_google_token)
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
        return result
        
    except Exception as e:
        logging.error(f"Error generating LOA for {request.business_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error generating LOA: {str(e)}")

@app.post("/api/generate-service-agreement")
def generate_service_agreement_endpoint(
    request: DocumentGenerationRequest,
    user_info: dict = Depends(verify_google_token)
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
        return result
        
    except Exception as e:
        logging.error(f"Error generating Service Agreement for {request.business_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error generating Service Agreement: {str(e)}")

@app.post("/api/generate-eoi")
def generate_eoi_endpoint(
    request: EOIGenerationRequest,
    user_info: dict = Depends(verify_google_token)
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
        return result
        
    except Exception as e:
        logging.error(f"Error generating EOI for {request.business_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error generating EOI: {str(e)}")

@app.post("/api/generate-ghg-offer")
def generate_ghg_offer_endpoint(
    request: DocumentGenerationRequest,
    user_info: dict = Depends(verify_google_token)
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

@app.get("/api/google/oauth-start")
async def google_oauth_start(scope: str = Query("presentations")):
    """Start Google OAuth flow for Presentations"""
    # You'll need to implement this based on your OAuth setup
    # This is a placeholder - you'll need to configure Google OAuth
    logging.info(f"Starting Google OAuth with scope: {scope}")
    
    # This should redirect to Google OAuth with appropriate scopes
    google_oauth_url = f"https://accounts.google.com/oauth/authorize"
    # Add your OAuth parameters here
    
    return {"oauth_url": google_oauth_url, "message": "Implement OAuth flow"}


@app.get("/api/eoi-types")
def get_eoi_types_endpoint(user_info: dict = Depends(verify_google_token)):
    """Get available Expression of Interest types"""
    return {
        "eoi_types": get_available_eoi_types(),
        "user_email": user_info.get("email")
    }

@app.post("/api/generate-loa-new")
def generate_loa_new_endpoint(
    request: NewLOAGeneration,
    user_info: dict = Depends(verify_google_token)
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
        return result
        
    except Exception as e:
        logging.error(f"Error generating new LOA for {request.business_name}: {str(e)}")
        return {
            "status": "error",
            "message": f"Error generating LOA: {str(e)}",
            "document_link": None,
            "user_email": user_info.get("email")
        }

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
    user_info: dict = Depends(verify_google_token)
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
