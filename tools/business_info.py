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
    Get file IDs directly from Google Sheets instead of n8n webhook.
    Reads from the 'Data from Airtable' sheet and filters by business name.
    
    Args:
        business_name: Name of the business to look up
        
    Returns:
        Dictionary with file IDs and status fields, or empty dict if not found/error
    """
    if not business_name:
        logger.warning("get_file_ids called with empty business_name")
        return {}
    
    logger.info(f"Fetching file IDs from Google Sheets for: {business_name}")
    logger.info(f"Sheet ID: {FILE_IDS_SHEET_ID}, Sheet Name: {FILE_IDS_SHEET_NAME}")
    
    # Get Google Sheets service
    service = get_sheets_service()
    if not service:
        logger.error("Could not create Google Sheets service")
        logger.warning("Falling back to empty dict - file IDs will not be available")
        return {}
    
    try:
        # Read all data from the sheet (including header row)
        result = service.spreadsheets().values().get(
            spreadsheetId=FILE_IDS_SHEET_ID,
            range=f"{FILE_IDS_SHEET_NAME}!A:ZZ",  # Read all columns
        ).execute()
        
        values = result.get('values', [])
        
        if not values or len(values) < 2:  # Need at least header + 1 data row
            logger.info(f"No data found in sheet for {business_name}")
            return {}
        
        # First row is the header - find the "Business Name" column index
        header_row = values[0]
        business_name_col_idx = None
        
        for idx, header in enumerate(header_row):
            if header and str(header).strip().lower() == "business name":
                business_name_col_idx = idx
                break
        
        if business_name_col_idx is None:
            logger.error("Could not find 'Business Name' column in sheet")
            return {}
        
        logger.info(f"Found 'Business Name' column at index {business_name_col_idx}")
        
        # Search for the matching business name in data rows
        matching_row = None
        search_business_name = business_name.strip()
        
        for row_idx, row in enumerate(values[1:], start=2):  # Start from row 2 (skip header)
            # Ensure row has enough columns
            while len(row) <= business_name_col_idx:
                row.append("")
            
            row_business_name = str(row[business_name_col_idx]).strip() if row[business_name_col_idx] else ""
            
            # Case-insensitive match
            if row_business_name.lower() == search_business_name.lower():
                matching_row = row
                logger.info(f"Found matching row at index {row_idx} for business: {row_business_name}")
                break
        
        if not matching_row:
            logger.info(f"No matching row found for business: {business_name}")
            return {}
        
        # Convert row to dictionary using header row as keys
        # Pad matching_row to match header length
        while len(matching_row) < len(header_row):
            matching_row.append("")
        
        file_ids_dict = {}
        for idx, header in enumerate(header_row):
            if idx < len(matching_row):
                value = matching_row[idx]
                # Only include non-empty values
                if value and str(value).strip():
                    file_ids_dict[str(header).strip()] = str(value).strip()
        
        logger.info(f"Successfully retrieved file IDs from Google Sheets. Found {len(file_ids_dict)} fields")
        return file_ids_dict
        
    except HttpError as e:
        logger.error(f"Google Sheets API HttpError: {e.status_code} - {e.reason}")
        if e.status_code == 404:
            logger.error(f"Sheet not found. Check that FILE_IDS_SHEET_ID and FILE_IDS_SHEET_NAME are correct")
            logger.error(f"Also ensure the service account has access to the sheet")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error reading from Google Sheets: {str(e)}")
        logger.exception(e)
        return {}

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
    # Send API request
    payload = {"business_name": business_name}
    logger.info(f"Making API call to n8n with payload: {payload}")
    
    try:
        response = requests.post(
            "https://membersaces.app.n8n.cloud/webhook/search-business-info", json=payload
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
        
        # Call the new tool to get file IDs
        file_ids_data = get_file_ids(official_business_name)

        # The webhook returns a list with one dictionary, so we extract it.
        file_ids_dict = {}
        if isinstance(file_ids_data, list) and file_ids_data:
            file_ids_dict = file_ids_data[0]
        elif isinstance(file_ids_data, dict):
            file_ids_dict = file_ids_data

        # Process file IDs for easy access by other functions
        # Map N8N keys to expected keys
        file_mapping = {
            'LOA File ID': 'business_LOA',
            'WIP': 'business_WIP',
            'Floor Plan': 'business_Floor Plan (Exit Map)',
            'Site Profiling': 'business_Site Profling',
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
            file_id = file_ids_dict.get(n8n_key)
            if file_id and file_id.strip():  # Check if file_id exists and is not empty
                file_link = f"https://drive.google.com/file/d/{file_id}/view"
                processed_file_ids[mapped_key] = file_link
        
        # NEW: Process all status fields from N8N data
        for n8n_key, mapped_key in status_mapping.items():
            status_value = file_ids_dict.get(n8n_key)
            if status_value and status_value.strip():  # Check if status exists and is not empty
                processed_file_ids[mapped_key] = status_value

        # Prepare LOA file link for Representative Details
        loa_file_id = file_ids_dict.get('LOA File ID')
        loa_file_link = None
        if loa_file_id and loa_file_id.strip():
            loa_file_link = f"https://drive.google.com/file/d/{loa_file_id}/view"

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
        
        # Check each document type based on N8N data
        doc_checks = [
            ('Initial Strategy', 'Initial Strategy'),
            ('Site Profling', 'Site Profiling'),  # Note the typo handling
            ('Service Fee Agreement', 'Service Fee Agreement'),
            ('Floor Plan (Exit Map)', 'Floor Plan'),
        ]
        
        for doc_name, n8n_key in doc_checks:
            file_id = file_ids_dict.get(n8n_key)
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
            
            if sc_file_id and sc_file_id.strip():
                sc_link = f"https://drive.google.com/file/d/{sc_file_id}/view"
                status_text = f" ({sc_status})" if sc_status else ""
                formatted_response += f"- **{sc_label}:** [In File]({sc_link}){status_text}\n"
            else:
                formatted_response += f"- **{sc_label}:** Not Available\n"

        wip_file_id = file_ids_dict.get('WIP')
        if wip_file_id and wip_file_id.strip():
            wip_file_link = f"https://drive.google.com/file/d/{wip_file_id}/view"
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