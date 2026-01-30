"""
One Month Savings Invoice Management
Handles logging invoices to Google Sheets and fetching invoice history
Uses Google Sheets API directly with service account (no n8n required)
"""

import requests
import logging
import re
import os
import json
from typing import Dict, List, Optional
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

# Google Sheets configuration
# These should be set as environment variables or in the service account key file
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service-account-key.json")
SHEET_ID = os.getenv("ONE_MONTH_SAVINGS_SHEET_ID", "")  # Google Sheet ID for invoice tracking
SHEET_NAME = os.getenv("ONE_MONTH_SAVINGS_SHEET_NAME", "Sheet1")  # Name of the sheet tab

# Fallback to n8n if direct API fails (optional)
USE_N8N_FALLBACK = os.getenv("USE_N8N_FALLBACK", "false").lower() == "true"
N8N_LOG_WEBHOOK = "https://membersaces.app.n8n.cloud/webhook/one-month-savings-log"
N8N_HISTORY_WEBHOOK = "https://membersaces.app.n8n.cloud/webhook/one-month-savings-history"


def get_sheets_service():
    """Get Google Sheets service using service account credentials"""
    try:
        logger.info("Attempting to create Google Sheets service...")
        logger.info(f"SERVICE_ACCOUNT_FILE: {SERVICE_ACCOUNT_FILE}")
        logger.info(f"SHEET_ID configured: {bool(SHEET_ID)}")
        logger.info(f"SHEET_NAME: {SHEET_NAME}")
        
        # Check if file exists
        file_exists = os.path.exists(SERVICE_ACCOUNT_FILE)
        logger.info(f"Service account file exists: {file_exists}")
        
        # Check environment variable
        service_account_info = os.getenv("SERVICE_ACCOUNT_JSON")
        has_env_json = bool(service_account_info)
        logger.info(f"SERVICE_ACCOUNT_JSON env var set: {has_env_json}")
        
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
                logger.exception(e)
        elif has_env_json:
            logger.info("Loading service account from SERVICE_ACCOUNT_JSON environment variable")
            try:
                # Try to parse the JSON
                if isinstance(service_account_info, str):
                    json_data = json.loads(service_account_info)
                else:
                    json_data = service_account_info
                
                logger.info(f"Service account email from JSON: {json_data.get('client_email', 'NOT FOUND')}")
                creds = Credentials.from_service_account_info(
                    json_data,
                    scopes=['https://www.googleapis.com/auth/spreadsheets']
                )
                logger.info("Successfully loaded service account from environment variable")
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in SERVICE_ACCOUNT_JSON: {str(e)}")
                logger.error(f"JSON length: {len(service_account_info) if service_account_info else 0}")
                return None
            except Exception as e:
                logger.error(f"Error loading service account from env var: {str(e)}")
                logger.exception(e)
                return None
        else:
            logger.error("No service account credentials found - neither file nor env var available")
            logger.error(f"File path checked: {os.path.abspath(SERVICE_ACCOUNT_FILE)}")
            logger.error(f"Environment variable SERVICE_ACCOUNT_JSON is: {type(service_account_info)}")
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
        logger.exception(e)
        return None


def log_invoice_to_sheets(invoice_data: Dict) -> Dict:
    """
    Log an invoice to Google Sheets directly using Google Sheets API
    
    Args:
        invoice_data: Dictionary containing invoice information
        
    Returns:
        Dict with success status and invoice number
    """
    try:
        # Validate required fields
        if not invoice_data.get("invoice_number") or not invoice_data.get("business_name"):
            return {
                "success": False,
                "error": "Missing required fields: invoice_number and business_name"
            }
        
        if not SHEET_ID:
            logger.warning("SHEET_ID not configured, falling back to n8n")
            logger.warning(f"ONE_MONTH_SAVINGS_SHEET_ID env var: {os.getenv('ONE_MONTH_SAVINGS_SHEET_ID', 'NOT SET')}")
            return _log_invoice_via_n8n(invoice_data)
        
        logger.info(f"Attempting to log invoice {invoice_data.get('invoice_number')} to sheet {SHEET_ID}")
        
        # Get Google Sheets service
        service = get_sheets_service()
        if not service:
            logger.error("Could not create Google Sheets service")
            logger.error("Available environment variables:")
            logger.error(f"  ONE_MONTH_SAVINGS_SHEET_ID: {bool(os.getenv('ONE_MONTH_SAVINGS_SHEET_ID'))}")
            logger.error(f"  ONE_MONTH_SAVINGS_SHEET_NAME: {os.getenv('ONE_MONTH_SAVINGS_SHEET_NAME', 'NOT SET')}")
            logger.error(f"  SERVICE_ACCOUNT_JSON: {bool(os.getenv('SERVICE_ACCOUNT_JSON'))}")
            logger.error(f"  SERVICE_ACCOUNT_FILE: {SERVICE_ACCOUNT_FILE}")
            
            if USE_N8N_FALLBACK:
                logger.warning("Falling back to n8n webhook")
                return _log_invoice_via_n8n(invoice_data)
            return {
                "success": False,
                "error": "Could not connect to Google Sheets - check logs for details"
            }
        
        # Flatten line items for the sheet
        line_items = invoice_data.get("line_items", [])
        line_items_summary = "; ".join([
            f"{item.get('solution_label', '')}: ${item.get('savings_amount', 0):.2f}"
            for item in line_items
        ]) if line_items else ""
        
        # Prepare row data for Google Sheets
        # Adjust column order to match your sheet structure
        row_data = [
            invoice_data.get("invoice_number", ""),
            invoice_data.get("business_name", ""),
            invoice_data.get("business_abn", ""),
            invoice_data.get("contact_name", ""),
            invoice_data.get("contact_email", ""),
            invoice_data.get("invoice_date", ""),
            invoice_data.get("due_date", ""),
            line_items_summary,
            f"{invoice_data.get('subtotal', 0):.2f}",
            f"{invoice_data.get('total_gst', 0):.2f}",
            f"{invoice_data.get('total_amount', 0):.2f}",
            invoice_data.get("status", "Generated"),
            invoice_data.get("created_at", ""),
            json.dumps(invoice_data.get("line_items", []))  # Store full line items as JSON
        ]
        
        # Append row to sheet
        body = {
            'values': [row_data]
        }
        
        logger.info(f"Logging invoice {invoice_data.get('invoice_number')} to Google Sheets")
        logger.info(f"Sheet ID: {SHEET_ID}")
        logger.info(f"Sheet Name: {SHEET_NAME}")
        logger.info(f"Row data columns: {len(row_data)}")
        
        try:
            result = service.spreadsheets().values().append(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!A:Z",  # Append to end of sheet
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            
            updated_rows = result.get('updates', {}).get('updatedRows', 'unknown')
            logger.info(f"Invoice {invoice_data.get('invoice_number')} logged successfully to row {updated_rows}")
            logger.info(f"API response: {result}")
        except HttpError as e:
            logger.error(f"Google Sheets API HttpError: {e.status_code} - {e.reason}")
            logger.error(f"Error details: {e.error_details if hasattr(e, 'error_details') else 'No details'}")
            logger.exception(e)
            raise
        except Exception as e:
            logger.error(f"Unexpected error writing to Google Sheets: {str(e)}")
            logger.exception(e)
            raise
        return {
            "success": True,
            "logged": True,
            "invoice_number": invoice_data.get("invoice_number")
        }
        
    except HttpError as e:
        logger.error(f"Google Sheets API error: {str(e)}")
        if USE_N8N_FALLBACK:
            logger.info("Falling back to n8n webhook")
            return _log_invoice_via_n8n(invoice_data)
        return {
            "success": False,
            "error": f"Google Sheets API error: {str(e)}"
        }
    except Exception as e:
        logger.error(f"Error logging invoice: {str(e)}")
        if USE_N8N_FALLBACK:
            logger.info("Falling back to n8n webhook")
            return _log_invoice_via_n8n(invoice_data)
        return {
            "success": False,
            "error": f"Failed to log invoice: {str(e)}"
        }


def _log_invoice_via_n8n(invoice_data: Dict) -> Dict:
    """Fallback method to log invoice via n8n webhook"""
    try:
        line_items = invoice_data.get("line_items", [])
        line_items_summary = "; ".join([
            f"{item.get('solution_label', '')}: ${item.get('savings_amount', 0):.2f}"
            for item in line_items
        ]) if line_items else ""
        
        sheet_payload = {
            "invoice_number": invoice_data.get("invoice_number"),
            "business_name": invoice_data.get("business_name"),
            "business_abn": invoice_data.get("business_abn", ""),
            "contact_name": invoice_data.get("contact_name", ""),
            "contact_email": invoice_data.get("contact_email", ""),
            "invoice_date": invoice_data.get("invoice_date"),
            "due_date": invoice_data.get("due_date"),
            "services": line_items_summary,
            "subtotal": f"{invoice_data.get('subtotal', 0):.2f}",
            "gst": f"{invoice_data.get('total_gst', 0):.2f}",
            "total_amount": f"{invoice_data.get('total_amount', 0):.2f}",
            "status": invoice_data.get("status", "Generated"),
            "created_at": invoice_data.get("created_at"),
            "line_items_json": str(invoice_data.get("line_items", []))
        }
        
        response = requests.post(N8N_LOG_WEBHOOK, json=sheet_payload, timeout=30)
        
        if response.status_code == 200:
            return {
                "success": True,
                "logged": True,
                "invoice_number": invoice_data.get("invoice_number")
            }
        else:
            return {
                "success": True,
                "logged": False,
                "message": "Invoice generated but logging failed"
            }
    except Exception as e:
        logger.error(f"n8n fallback error: {str(e)}")
        return {
            "success": True,
            "logged": False,
            "message": "Invoice generated but logging failed"
        }


def get_invoice_history(business_name: str) -> Dict:
    """
    Get invoice history for a business from Google Sheets directly using Google Sheets API
    
    Args:
        business_name: Name of the business to get invoices for
        
    Returns:
        Dict with invoices list and count
    """
    try:
        if not business_name:
            return {
                "invoices": [],
                "error": "Missing required field: business_name"
            }
        
        if not SHEET_ID:
            logger.warning("SHEET_ID not configured, falling back to n8n")
            logger.warning(f"ONE_MONTH_SAVINGS_SHEET_ID env var: {os.getenv('ONE_MONTH_SAVINGS_SHEET_ID', 'NOT SET')}")
            return _get_invoice_history_via_n8n(business_name)
        
        logger.info(f"Fetching invoice history for: {business_name}")
        logger.info(f"Sheet ID: {SHEET_ID}, Sheet Name: {SHEET_NAME}")
        
        # Get Google Sheets service
        service = get_sheets_service()
        if not service:
            logger.error("Could not create Google Sheets service for history fetch")
            logger.error("Available environment variables:")
            logger.error(f"  ONE_MONTH_SAVINGS_SHEET_ID: {bool(os.getenv('ONE_MONTH_SAVINGS_SHEET_ID'))}")
            logger.error(f"  ONE_MONTH_SAVINGS_SHEET_NAME: {os.getenv('ONE_MONTH_SAVINGS_SHEET_NAME', 'NOT SET')}")
            logger.error(f"  SERVICE_ACCOUNT_JSON: {bool(os.getenv('SERVICE_ACCOUNT_JSON'))}")
            
            if USE_N8N_FALLBACK:
                logger.warning("Falling back to n8n webhook")
                return _get_invoice_history_via_n8n(business_name)
            return {
                "invoices": [],
                "error": "Could not connect to Google Sheets - check logs for details"
            }
        
        # Read all data from the sheet
        # Adjust range based on your sheet structure (assuming headers in row 1)
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A2:Z"  # Skip header row, read all data
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            logger.info(f"No data found in sheet for {business_name}")
            return {
                "invoices": [],
                "count": 0
            }
        
        # Map column indices (adjust based on your sheet structure)
        # Assuming columns: Invoice Number, Business Name, Business ABN, Contact Name, Contact Email,
        # Invoice Date, Due Date, Services, Subtotal, GST, Total Amount, Status, Created At, Line Items JSON
        invoices = []
        
        for row in values:
            # Ensure row has enough columns (pad with empty strings if needed)
            while len(row) < 14:
                row.append("")
            
            # Filter by business name (assuming business name is in column B, index 1)
            if len(row) > 1 and row[1].strip().lower() == business_name.strip().lower():
                # Parse line items from JSON string (column 13, index 13)
                line_items = []
                if len(row) > 13 and row[13]:
                    try:
                        line_items_data = json.loads(row[13])
                        line_items = [{"solution_label": item.get("solution_label", "")} for item in line_items_data]
                    except:
                        # If JSON parsing fails, try to parse from services column (column 8, index 8)
                        if len(row) > 8 and row[8]:
                            services = row[8].split(";")
                            line_items = [{"solution_label": s.split(":")[0].strip()} for s in services if ":" in s]
                
                # Parse amounts
                # Column mapping: 0=Invoice Number, 1=Business Name, 2=ABN, 3=Contact Name, 4=Email,
                # 5=Invoice Date, 6=Due Date, 7=Services, 8=Subtotal, 9=GST, 10=Total Amount, 11=Status, 12=Created At, 13=Line Items JSON
                total_amount = 0
                if len(row) > 10 and row[10]:
                    try:
                        total_amount = float(str(row[10]).replace("$", "").replace(",", ""))
                    except:
                        pass
                
                subtotal = 0
                if len(row) > 8 and row[8]:
                    try:
                        subtotal = float(str(row[8]).replace("$", "").replace(",", ""))
                    except:
                        subtotal = total_amount / 1.1 if total_amount > 0 else 0
                
                total_gst = 0
                if len(row) > 9 and row[9]:
                    try:
                        total_gst = float(str(row[9]).replace("$", "").replace(",", ""))
                    except:
                        total_gst = total_amount * 0.1 / 1.1 if total_amount > 0 else 0
                
                invoice = {
                    "invoice_number": row[0] if len(row) > 0 else "",
                    "business_name": row[1] if len(row) > 1 else "",
                    "business_abn": row[2] if len(row) > 2 else "",
                    "contact_name": row[3] if len(row) > 3 else "",
                    "contact_email": row[4] if len(row) > 4 else "",
                    "invoice_date": row[5] if len(row) > 5 else "",
                    "due_date": row[6] if len(row) > 6 else "",
                    "subtotal": subtotal,
                    "total_gst": total_gst,
                    "total_amount": total_amount,
                    "status": row[11] if len(row) > 11 else "Generated",
                    "created_at": row[12] if len(row) > 12 else "",
                    "line_items": line_items
                }
                invoices.append(invoice)
        
        logger.info(f"Found {len(invoices)} invoices for {business_name}")
        return {
            "invoices": invoices,
            "count": len(invoices)
        }
        
    except HttpError as e:
        logger.error(f"Google Sheets API error: {str(e)}")
        if USE_N8N_FALLBACK:
            logger.info("Falling back to n8n webhook")
            return _get_invoice_history_via_n8n(business_name)
        return {
            "invoices": [],
            "error": f"Google Sheets API error: {str(e)}"
        }
    except Exception as e:
        logger.error(f"Error fetching invoice history: {str(e)}")
        if USE_N8N_FALLBACK:
            logger.info("Falling back to n8n webhook")
            return _get_invoice_history_via_n8n(business_name)
        return {
            "invoices": [],
            "error": f"Failed to fetch history: {str(e)}"
        }


def _get_invoice_history_via_n8n(business_name: str) -> Dict:
    """Fallback method to get invoice history via n8n webhook"""
    try:
        response = requests.post(
            N8N_HISTORY_WEBHOOK,
            json={"business_name": business_name},
            timeout=30
        )
        
        if response.status_code != 200:
            return {
                "invoices": [],
                "message": "Could not fetch invoice history"
            }
        
        data = response.json()
        rows = data if isinstance(data, list) else []
        invoices = []
        
        for row in rows:
            solution = row.get("Solution ") or row.get("Solution") or ""
            amount = 0
            amount_value = row.get("Amount")
            if isinstance(amount_value, (int, float)):
                amount = float(amount_value)
            elif isinstance(amount_value, str):
                amount = float(amount_value.replace("$", "").replace(",", "")) or 0
            
            invoice = {
                "invoice_number": row.get("Invoice Number") or row.get("invoice_number") or "",
                "business_name": row.get("Member") or row.get("business_name") or "",
                "business_abn": row.get("Business ABN") or row.get("business_abn") or "",
                "contact_name": row.get("Contact Name") or row.get("contact_name") or "",
                "contact_email": row.get("Contact Email") or row.get("contact_email") or "",
                "invoice_date": row.get("Invoice Date") or row.get("invoice_date") or "",
                "due_date": row.get("Due Date") or row.get("due_date") or "",
                "subtotal": row.get("Subtotal") if row.get("Subtotal") else amount / 1.1,
                "total_gst": row.get("GST") if row.get("GST") else amount * 0.1 / 1.1,
                "total_amount": amount,
                "status": row.get("Status") or row.get("status") or "Generated",
                "created_at": row.get("Created At") or row.get("created_at") or "",
                "line_items": [{"solution_label": solution.strip()}] if solution else []
            }
            invoices.append(invoice)
        
        return {
            "invoices": invoices,
            "count": len(invoices)
        }
    except Exception as e:
        logger.error(f"n8n fallback error: {str(e)}")
        return {
            "invoices": [],
            "error": f"Failed to fetch history: {str(e)}"
        }


def get_next_invoice_number() -> str:
    """
    Get the next sequential invoice number by fetching all invoices
    and finding the highest number, then incrementing it.
    
    Returns:
        Next invoice number in format RA####
    """
    try:
        # Get all invoices by querying with empty business name or a wildcard
        # Since n8n returns invoices for a specific business, we need to get all
        # For now, we'll fetch recent invoices and find the max
        # Note: This assumes invoices are stored sequentially in the sheet
        
        # Try to get invoices for a common business name or use a different approach
        # For sequential numbering, we might need to query all invoices
        # Since we don't have a "get all" endpoint, we'll use a workaround:
        # Get invoices for a few known businesses and find the max
        
        # Alternative: Store the last invoice number in a separate field or use a counter
        # For now, we'll generate based on timestamp + random to avoid collisions
        # and let the user know they should check for duplicates
        
        from datetime import datetime
        
        # Try to get recent invoices by querying with a partial match
        # Since we can't get all invoices easily, we'll use a timestamp-based approach
        # with a check against recent invoices
        
        # Get current timestamp for uniqueness
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Generate a number based on timestamp (last 4 digits)
        number = int(timestamp[-4:])
        
        # Format as RA####
        invoice_number = f"RA{number:04d}"
        
        logger.info(f"Generated invoice number: {invoice_number}")
        return invoice_number
        
    except Exception as e:
        logger.error(f"Error generating invoice number: {str(e)}")
        # Fallback to random number
        import random
        number = random.randint(1000, 9999)
        return f"RA{number}"


def get_next_sequential_invoice_number(business_name: Optional[str] = None) -> str:
    """
    Get the next sequential invoice number by checking existing invoices from Google Sheets.
    Reads all invoices to find the highest number globally (not per business).
    
    Args:
        business_name: Optional (not used for sequential numbering, but kept for API compatibility)
        
    Returns:
        Next invoice number in format RA####
    """
    try:
        max_number = 0
        
        if not SHEET_ID:
            logger.warning("SHEET_ID not configured, using fallback numbering")
            # Fallback: try to get from n8n if available
            if business_name:
                history = _get_invoice_history_via_n8n(business_name)
                invoices = history.get("invoices", [])
                for invoice in invoices:
                    inv_num = invoice.get("invoice_number", "")
                    match = re.search(r'RA(\d+)', inv_num)
                    if match:
                        num = int(match.group(1))
                        max_number = max(max_number, num)
        else:
            # Get Google Sheets service
            service = get_sheets_service()
            if service:
                # Read all invoice numbers from the sheet
                result = service.spreadsheets().values().get(
                    spreadsheetId=SHEET_ID,
                    range=f"{SHEET_NAME}!A2:A"  # Read all invoice numbers (column A)
                ).execute()
                
                values = result.get('values', [])
                
                # Extract numbers from all invoice numbers
                for row in values:
                    if row and len(row) > 0:
                        inv_num = str(row[0]).strip()
                        match = re.search(r'RA(\d+)', inv_num)
                        if match:
                            num = int(match.group(1))
                            max_number = max(max_number, num)
            else:
                logger.warning("Could not create Google Sheets service, using fallback")
                if business_name:
                    history = _get_invoice_history_via_n8n(business_name)
                    invoices = history.get("invoices", [])
                    for invoice in invoices:
                        inv_num = invoice.get("invoice_number", "")
                        match = re.search(r'RA(\d+)', inv_num)
                        if match:
                            num = int(match.group(1))
                            max_number = max(max_number, num)
        
        # Always increment by 1
        next_number = max_number + 1
        
        # Ensure it's at least 1000 (for 4-digit numbers)
        if next_number < 1000:
            next_number = 1000
        
        invoice_number = f"RA{next_number:04d}"
        logger.info(f"Generated sequential invoice number: {invoice_number} (previous max: {max_number})")
        return invoice_number
        
    except Exception as e:
        logger.error(f"Error generating sequential invoice number: {str(e)}")
        # Fallback
        import random
        number = random.randint(1000, 9999)
        return f"RA{number}"

