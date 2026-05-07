import requests
import logging
import os
import json
from typing import Optional, Dict
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Load environment variables
backend_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(backend_root, '.env')
load_dotenv(dotenv_path=env_path)

logger = logging.getLogger(__name__)

# Google Sheets configuration for file IDs
FILE_IDS_SHEET_ID = os.getenv("FILE_IDS_SHEET_ID", "1l_ShkAcpS1HBqX8EdXLEVmn3pkliVGwsskkkI0GlLho")
FILE_IDS_SHEET_NAME = os.getenv("FILE_IDS_SHEET_NAME", "Data from Airtable")  # Sheet name or can use GID
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service-account-key.json")

def _normalize_drive_cell_value(raw_value: object) -> str:
    """Normalize comma-separated Google Drive IDs/URLs into comma-separated URLs."""
    if raw_value is None:
        return ""
    raw_text = str(raw_value).strip()
    if not raw_text:
        return ""

    normalized_parts = []
    for part in raw_text.split(","):
        token = part.strip()
        if not token:
            continue

        # Already a URL.
        if token.startswith("http://") or token.startswith("https://"):
            normalized_parts.append(token)
            continue

        # Handle malformed legacy values like "<id>/view" or "<id>/edit".
        token = token.split("/", 1)[0].strip()
        if not token:
            continue

        normalized_parts.append(f"https://drive.google.com/file/d/{token}/view")

    return ",".join(normalized_parts)

def get_sheets_service():
    """Get Google Sheets service using service account credentials"""
    try:
        logger.info("Attempting to create Google Sheets service for file IDs...")
        
        # Check if file exists
        file_exists = os.path.exists(SERVICE_ACCOUNT_FILE)
        
        # Check environment variable
        service_account_info = os.getenv("SERVICE_ACCOUNT_JSON")
        has_env_json = bool(service_account_info)
        
        creds = None
        
        # Load service account credentials
        if file_exists:
            logger.info(f"Loading service account from file: {SERVICE_ACCOUNT_FILE}")
            try:
                creds = Credentials.from_service_account_file(
                    SERVICE_ACCOUNT_FILE,
                    scopes=['https://www.googleapis.com/auth/spreadsheets']
                )
                logger.info("Successfully loaded service account from file")
            except Exception as e:
                logger.error(f"Error loading service account from file: {str(e)}")
                return None
        elif has_env_json:
            logger.info("Loading service account from SERVICE_ACCOUNT_JSON environment variable")
            try:
                # Try to parse the JSON
                if isinstance(service_account_info, str):
                    json_data = json.loads(service_account_info)
                else:
                    json_data = service_account_info
                
                creds = Credentials.from_service_account_info(
                    json_data,
                    scopes=['https://www.googleapis.com/auth/spreadsheets']
                )
                logger.info("Successfully loaded service account from environment variable")
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in SERVICE_ACCOUNT_JSON: {str(e)}")
                return None
            except Exception as e:
                logger.error(f"Error loading service account from env var: {str(e)}")
                return None
        else:
            logger.error("No service account credentials found - neither file nor env var available")
            return None
        
        if not creds:
            logger.error("Failed to create credentials object")
            return None
        
        logger.info("Building Google Sheets API service...")
        service = build('sheets', 'v4', credentials=creds)
        logger.info("Google Sheets service created successfully")
        return service
        
    except Exception as e:
        logger.error(f"Error creating Google Sheets service: {str(e)}")
        return None

def get_file_ids(business_name: str) -> dict:
    """
    Get file IDs from n8n webhook (return_fileIDs). Used by get_business_information.
    """
    if not business_name:
        return {}
    try:
        response = requests.post(
            "https://membersaces.app.n8n.cloud/webhook/return_fileIDs",
            json={"business_name": business_name.strip()},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list) and data:
            return data[0] if isinstance(data[0], dict) else {}
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning("Failed to get file IDs from n8n webhook: %s", e)
        return {}


# Base 1 Landing Page Responses sheet (interface form submissions)
BASE1_LANDING_SHEET_ID = os.getenv("BASE1_LANDING_SHEET_ID", "1FNQXlecyp-qrzao2TOzbndKoCfUPGFN_HgbkNmAs0jw")
BASE1_LANDING_SHEET_NAME = os.getenv("BASE1_LANDING_SHEET_NAME", "Landing Page Responses")


def get_base1_landing_responses() -> list:
    """
    Get all rows from the Base 1 Landing Page Responses Google Sheet.
    Returns a list of dicts (one per row) with columns: Company Name, Contact Name,
    Contact Email, Contact Number, State, Timestamp, Google Drive Folder, Base 1 Review, Utility Types.
    Email HTML column is excluded from the response.
    """
    service = get_sheets_service()
    if not service:
        logger.error("Could not create Google Sheets service for Base 1 landing responses")
        return []
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=BASE1_LANDING_SHEET_ID,
            range=f"'{BASE1_LANDING_SHEET_NAME}'!A:J",
        ).execute()
        rows = result.get("values", [])
        if not rows:
            return []
        headers = [str(h).strip() for h in rows[0]]
        exclude = {"Email HTML", "email_html"}
        out = []
        for row in rows[1:]:
            obj = {}
            for i, key in enumerate(headers):
                if key in exclude:
                    continue
                obj[key] = row[i] if i < len(row) else ""
            out.append(obj)
        return out
    except HttpError as e:
        logger.error(f"Google Sheets API error reading Base 1 landing responses: {e.resp.status_code} - {e.reason}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error reading Base 1 landing responses: {str(e)}")
        logger.exception(e)
        return []


def get_business_information(business_name: str) -> dict:
    """
    Get the business information including:
    - record_ID
    - Business Details: Name, Trading Name, ABN
    - Contact Information: Postal Address, Site Address, Telephone, Email
    - Representative Details: Contact Name, Position, LOA Sign Date
    - Business Documents: List of available documents
    - Linked Utilities and their retailers
    - Google Drive folder URL

    Args:
        business_name (str): The name of the business to search for

    Returns:
        Dict containing business information including record_ID and formatted_output
    """
    logger = logging.getLogger(__name__)

    processed_file_ids = {}
    # Send API request to n8n
    payload = {"business_name": business_name}
    logger.info(f"Making API call to n8n with payload: {payload}")
    
    try:
        response = requests.post(
            "https://membersaces.app.n8n.cloud/webhook/search-business-info-test", json=payload
        )
        logger.info(f"API response status code: {response.status_code}")
        logger.info(f"API response content: {response.text}")

        if response.status_code == 404:
            return {"_formatted_output": "Sorry but couldn't find that business name"}

        if not response.text:
            logger.error("Empty response received from API")
            return {"_formatted_output": "Error: Received empty response from the server"}

        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {str(e)}")
            logger.error(f"Raw response content: {response.text}")
            return {"_formatted_output": "Error: Invalid response format from the server"}

        logger.info(f"Parsed API response data: {data}")

        # Get the official business name to use for file ID lookup
        official_business_name = data.get('business_details', {}).get('name', business_name)

        # Get file IDs from n8n webhook (return_fileIDs)
        file_ids_data = get_file_ids(official_business_name)

        # Webhook may return a list with one dictionary or a single dict
        file_ids_dict = {}
        if isinstance(file_ids_data, list) and file_ids_data:
            file_ids_dict = file_ids_data[0]
        elif isinstance(file_ids_data, dict):
            file_ids_dict = file_ids_data

        # Process file IDs for easy access by other functions
        # Map N8N keys to expected keys (support both "Site Profling" and "Site Profiling" from n8n)
        file_mapping = {
            'LOA File ID': 'business_LOA',
            'WIP': 'business_WIP',
            'Floor Plan': 'business_Floor Plan (Exit Map)',
            'Site Profling': 'business_Site Profiling',
            'Site Profiling': 'business_Site Profiling',
            'Service Fee Agreement': 'business_Service Fee Agreement',
            'Initial Strategy': 'business_Initial Strategy',
            'Cleaning Invoice': 'invoice_Cleaning',
            'Telecommunication:': 'invoice_Telecommunication',
            'Oil Invoice': 'invoice_Oil',
            'Water Invoice': 'invoice_Water',
            'SC C&I E': 'contract_C&I Electricity',
            'SC SME E': 'contract_SME Electricity',
            'SC C&I G': 'contract_C&I Gas',
            'SC SME G': 'contract_SME Gas',
            'SC Waste': 'contract_Waste',
            'SC Oil': 'contract_Oil',
            'SC DMA': 'contract_DMA',
            'Amortisation Excel': 'business_amortisation_excel',
            'Amortisation PDF': 'business_amortisation_pdf',
        }
        
        # NEW: Status field mapping
        status_mapping = {
            'SC C&I E Status:': 'contract_C&I Electricity_status',
            'SC SME E Status:': 'contract_SME Electricity_status',
            'SC C&I G Status:': 'contract_C&I Gas_status',
            'SC SME G Status:': 'contract_SME Gas_status',
            'SC Waste Status:': 'contract_Waste_status',
            'SC Oil Status:': 'contract_Oil_status',
            'SC DMA Status:': 'contract_DMA_status',
        }
        
        # Process all file IDs from N8N data
        for n8n_key, mapped_key in file_mapping.items():
            normalized_value = _normalize_drive_cell_value(file_ids_dict.get(n8n_key))
            if normalized_value:
                processed_file_ids[mapped_key] = normalized_value
        
        # NEW: Process all status fields from N8N data
        for n8n_key, mapped_key in status_mapping.items():
            status_value = file_ids_dict.get(n8n_key)
            if status_value and status_value.strip():  # Check if status exists and is not empty
                processed_file_ids[mapped_key] = status_value

        # Prepare LOA file link for Representative Details
        loa_file_id = file_ids_dict.get('LOA File ID')
        loa_file_link = _normalize_drive_cell_value(loa_file_id)

        # Format the response message in a clear and organized way
        formatted_response = f"""Here is the information for {business_name}:

### Business Details:
- **Business Name:** {data.get('business_details', {}).get('name', 'N/A')}
- **Trading As:** {data.get('business_details', {}).get('trading_name', 'N/A')}
- **ABN:** {data.get('business_details', {}).get('abn', 'N/A')}

### Contact Information:
- **Postal Address:** {data.get('contact_information', {}).get('postal_address', 'N/A')}
- **Site Address:** {data.get('contact_information', {}).get('site_address', 'N/A')}
- **Contact Number:** {data.get('contact_information', {}).get('telephone', 'N/A')}
- **Contact Email:** {data.get('contact_information', {}).get('email', 'N/A')}

### Representative Details:
- **Contact Name:** {data.get('representative_details', {}).get('contact_name', 'N/A')}
- **Position:** {data.get('representative_details', {}).get('position', 'N/A')}
- **LOA Sign Date:** {data.get('representative_details', {}).get('signed_date', 'N/A')}
"""
        # Add LOA file link if available (constructed from ID only)
        if loa_file_link:
            formatted_response += f"- **LOA:** [In File]({loa_file_link})\n"
        else:
            formatted_response += f"- **LOA:** Not Available\n"

        formatted_response += "\n### Business Documents:\n"
        
        # Create business documents dict from available files
        business_documents = {}
        
        # Check each document type based on N8N data (use correct "Site Profiling" as key)
        doc_checks = [
            ('Initial Strategy', 'Initial Strategy'),
            ('Site Profiling', 'Site Profiling'),  # try both spellings for file_id below
            ('Service Fee Agreement', 'Service Fee Agreement'),
            ('Floor Plan (Exit Map)', 'Floor Plan'),
        ]

        for doc_name, n8n_key in doc_checks:
            file_id = file_ids_dict.get(n8n_key) or (file_ids_dict.get('Site Profling') if n8n_key == 'Site Profiling' else None)
            business_documents[doc_name] = bool(file_id and file_id.strip())
        
        # Always show Initial Strategy as available (as per original logic)
        if 'Initial Strategy' not in business_documents:
            business_documents['Initial Strategy'] = True
            
        if business_documents:
            for doc_name, status in business_documents.items():
                if status:
                    # Map document name to file ID key
                    doc_key = f"business_{doc_name}"
                    file_link = processed_file_ids.get(doc_key)

                    if file_link:
                        formatted_response += f"- **{doc_name}:** [In File]({file_link})\n"
                    else:
                        formatted_response += f"- **{doc_name}:** In File (Link not found)\n"
                else:
                    formatted_response += f"- **{doc_name}:** Not Available\n"
        else:
            formatted_response += "- No business documents available\n"

        # Add Signed Contracts section WITH STATUS
        sc_fields = [
            ("SC C&I E", "C&I Electricity", "SC C&I E Status:"),
            ("SC SME E", "SME Electricity", "SC SME E Status:"),
            ("SC C&I G", "C&I Gas", "SC C&I G Status:"),
            ("SC SME G", "SME Gas", "SC SME G Status:"),
            ("SC Waste", "Waste", "SC Waste Status:"),
            ("SC Oil", "Oil", "SC Oil Status:"),
            ("SC DMA", "DMA", "SC DMA Status:"),
        ]
        formatted_response += "\n### Signed Contracts:\n"
        for sc_key, sc_label, status_key in sc_fields:
            sc_file_id = file_ids_dict.get(sc_key)
            sc_status = file_ids_dict.get(status_key, "")

            normalized_sc_value = _normalize_drive_cell_value(sc_file_id)
            if normalized_sc_value:
                sc_links = [v.strip() for v in normalized_sc_value.split(",") if v.strip()]
                status_values = [v.strip() for v in str(sc_status).split(",") if v.strip()] if sc_status else []
                if len(sc_links) == 1:
                    status_text = f" ({status_values[0]})" if status_values else ""
                    formatted_response += f"- **{sc_label}:** [In File]({sc_links[0]}){status_text}\n"
                else:
                    formatted_response += f"- **{sc_label}:**\n"
                    for idx, link in enumerate(sc_links):
                        status_text = ""
                        if status_values:
                            status_text = f" ({status_values[idx] if idx < len(status_values) else status_values[0]})"
                        formatted_response += f"  - [In File #{idx + 1}]({link}){status_text}\n"
            else:
                formatted_response += f"- **{sc_label}:** Not Available\n"

        wip_file_id = file_ids_dict.get('WIP')
        wip_file_link = _normalize_drive_cell_value(wip_file_id)
        if wip_file_link:
            processed_file_ids["business_WIP"] = wip_file_link

        formatted_response += "\n### Linked Utilities and Retailers:"

        # Add linked utilities and their details
        linked_utilities = data.get('Linked_Details', {}).get('linked_utilities', {})
        utility_retailers = data.get('Linked_Details', {}).get('utility_retailers', {})

        if "Robot Number" in linked_utilities:
            linked_utilities["Robot"] = linked_utilities["Robot Number"]
        
        # Handle all utility types (keeping the original logic)
        utility_types = [
            ('C&I Electricity', 'NMI'),
            ('SME Electricity', 'NMI'),
            ('C&I Gas', 'MRIN'),
            ('SME Gas', 'MRIN'),
            ('Small Gas', 'MRIN'),
            ('Waste', 'Account Number'),
            ('Oil', 'Account Name'),
        ]

        for utility_type, identifier_type in utility_types:
            if utility_type in linked_utilities:
                formatted_response += f"\n\n**{utility_type}:**"
                details = linked_utilities[utility_type]
                if isinstance(details, str):
                    formatted_response += f"\n- {identifier_type}: {details}"
                elif isinstance(details, bool) and details:
                    formatted_response += "\n- Status: In File"
                if utility_type in utility_retailers:
                    retailers = utility_retailers[utility_type]
                    if isinstance(retailers, list):
                        formatted_response += f"\n- Retailer: {', '.join(retailers)}"
                    else:
                        formatted_response += f"\n- Retailer: {retailers}"

        # Handle Robots section
        robot_number = linked_utilities.get('Robot Number')
        robot_supplier = utility_retailers.get('Robot Supplier')
        if robot_number or robot_supplier:
            formatted_response += "\n\n**Robots:**"
            if robot_number:
                if isinstance(robot_number, str) and ',' in robot_number:
                    robot_numbers = [r.strip() for r in robot_number.split(',')]
                    formatted_response += f"\n- Robot Number: {', '.join(robot_numbers)}"
                elif isinstance(robot_number, list):
                    formatted_response += f"\n- Robot Number: {', '.join(robot_number)}"
                else:
                    formatted_response += f"\n- Robot Number: {robot_number}"
            if robot_supplier:
                formatted_response += f"\n- Supplier: {robot_supplier}"

        # Handle Cleaning
        if 'Cleaning' in linked_utilities:
            formatted_response += "\n\n**Cleaning:**"
            details = linked_utilities['Cleaning']
            if isinstance(details, bool):
                formatted_response += f"\n- Status: {'In File' if details else 'Not Available'}"
        else:
            formatted_response += "\n\n**Cleaning:**\n- Status: Not Available"

        # Handle Telecommunication
        if 'Telecommunication' in linked_utilities:
            formatted_response += "\n\n**Telecommunication:**"
            details = linked_utilities['Telecommunication']
            if isinstance(details, bool):
                formatted_response += f"\n- Status: {'In File' if details else 'Not Available'}"
        else:
            formatted_response += "\n\n**Telecommunication:**\n- Status: Not Available"

        formatted_response += f"""

### Google Drive:
- **Folder URL:** {data.get('gdrive', {}).get('folder_url', 'N/A')}

### Information Retrieval
"""

        logger.info(f"Formatted response: {formatted_response}")
        logger.info(f"Processed file IDs: {processed_file_ids}")
        
        # Return both the raw data and formatted output, plus processed file IDs
        return {
            **data,  # Include all raw data
            "_formatted_output": formatted_response,  # Add formatted output
            "_processed_file_ids": processed_file_ids,  # Add processed file IDs for other functions
            "business_documents": business_documents  # Add the business_documents dict
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error in get_business_information: {str(e)}", exc_info=True)
        return {"_formatted_output": "Error: Unable to connect to the server. Please try again later."}
    except Exception as e:
        logger.error(f"Unexpected error in get_business_information: {str(e)}", exc_info=True)
        return {"_formatted_output": "Error: An unexpected error occurred. Please try again later."}