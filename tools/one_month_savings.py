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
        
        # Sheet structure: A=Member, B=Solution, C=Savings Amount, D=GST, E=Total Invoice, F=Invoice Number, G=Due Date
        # Each line item gets its own row
        line_items = invoice_data.get("line_items", [])
        
        if not line_items:
            logger.warning("No line items to log")
            return {
                "success": False,
                "error": "No line items provided"
            }
        
        # Prepare rows - one row per line item
        rows_data = []
        for item in line_items:
            solution_label = item.get('solution_label', '').strip()
            savings_amount = item.get('savings_amount', 0)
            gst = savings_amount * 0.1 / 1.1  # Calculate GST (10% of total, so GST = total * 0.1 / 1.1)
            total = savings_amount + gst
            
            row_data = [
                invoice_data.get("business_name", ""),      # Column A: Member
                solution_label,                              # Column B: Solution (no amount)
                f"${savings_amount:.2f}",                    # Column C: Savings Amount
                f"${gst:.2f}",                              # Column D: GST
                f"${total:.2f}",                            # Column E: Total Invoice
                invoice_data.get("invoice_number", ""),      # Column F: Invoice Number
                invoice_data.get("due_date", ""),            # Column G: Due Date
            ]
            rows_data.append(row_data)
        
        # Append all rows to sheet
        body = {
            'values': rows_data
        }
        
        logger.info(f"Logging invoice {invoice_data.get('invoice_number')} to Google Sheets")
        logger.info(f"Sheet ID: {SHEET_ID}")
        logger.info(f"Sheet Name: {SHEET_NAME}")
        logger.info(f"Number of line items: {len(rows_data)}")
        
        try:
            result = service.spreadsheets().values().append(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!A:G",  # Append to columns A-G
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            
            updated_rows = result.get('updates', {}).get('updatedRows', 'unknown')
            logger.info(f"Invoice {invoice_data.get('invoice_number')} logged successfully - {updated_rows} rows added")
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
        # Sheet structure: A=Member, B=Solution, C=Savings Amount, D=GST, E=Total Invoice, F=Invoice Number, G=Due Date
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A2:G"  # Skip header row, read columns A-G
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            logger.info(f"No data found in sheet for {business_name}")
            return {
                "invoices": [],
                "count": 0
            }
        
        logger.info(f"Processing {len(values)} rows from sheet")
        logger.info(f"Searching for business: '{business_name}'")
        
        # Group rows by invoice number (each row is a line item)
        # Column mapping: 0=Member (Business Name), 1=Solution, 2=Savings Amount, 3=GST, 4=Total Invoice, 5=Invoice Number, 6=Due Date
        invoice_dict = {}  # Key: invoice_number, Value: invoice data with line_items array
        
        for idx, row in enumerate(values):
            # Ensure row has enough columns
            while len(row) < 7:
                row.append("")
            
            # Filter by business name (column A, index 0)
            row_business_name = row[0].strip() if len(row) > 0 else ""
            search_business_name = business_name.strip()
            
            # Log first few rows for debugging
            if idx < 3:
                logger.info(f"Row {idx}: Business='{row_business_name}', Invoice='{row[5] if len(row) > 5 else 'N/A'}'")
            
            if len(row) > 0 and row_business_name.lower() == search_business_name.lower():
                invoice_number = row[5].strip() if len(row) > 5 else ""
                if not invoice_number:
                    continue
                
                # Parse solution (column B) - remove any amount that might be in it
                solution = row[1].strip() if len(row) > 1 else ""
                # Remove patterns like ": $2000.00" or ": 2000" from solution
                solution = re.sub(r':\s*\$?[\d,]+\.?\d*', '', solution).strip()
                
                # Parse savings amount (column C)
                savings_amount = 0
                if len(row) > 2 and row[2]:
                    try:
                        savings_amount = float(str(row[2]).replace("$", "").replace(",", ""))
                    except:
                        pass
                
                # Parse GST (column D)
                gst = 0
                if len(row) > 3 and row[3]:
                    try:
                        gst = float(str(row[3]).replace("$", "").replace(",", ""))
                    except:
                        pass
                
                # Parse total invoice (column E)
                total_amount = 0
                if len(row) > 4 and row[4]:
                    try:
                        total_amount = float(str(row[4]).replace("$", "").replace(",", ""))
                    except:
                        # If total not provided, calculate from savings + GST
                        total_amount = savings_amount + gst
                
                # Parse due date (column G, index 6)
                due_date = row[6].strip() if len(row) > 6 else ""
                
                # Create line item
                line_item = {
                    "solution_label": solution,
                    "savings_amount": savings_amount
                }
                
                # Group by invoice number
                if invoice_number not in invoice_dict:
                    invoice_dict[invoice_number] = {
                        "invoice_number": invoice_number,
                        "business_name": row_business_name,
                        "business_abn": "",
                        "contact_name": "",
                        "contact_email": "",
                        "invoice_date": "",
                        "due_date": due_date,
                        "subtotal": 0,
                        "total_gst": 0,
                        "total_amount": 0,
                        "status": "Generated",
                        "created_at": "",
                        "line_items": []
                    }
                
                # Add line item and accumulate amounts
                invoice_dict[invoice_number]["line_items"].append(line_item)
                invoice_dict[invoice_number]["subtotal"] += savings_amount
                invoice_dict[invoice_number]["total_gst"] += gst
                invoice_dict[invoice_number]["total_amount"] += total_amount if total_amount > 0 else (savings_amount + gst)
        
        # Convert dict to list
        invoices = list(invoice_dict.values())
        
        logger.info(f"Found {len(invoices)} invoices for {business_name}")
        if len(invoices) == 0:
            logger.warning(f"No invoices found for business: '{business_name}'")
            logger.warning(f"Total rows checked: {len(values)}")
            if len(values) > 0:
                sample_names = [row[0] if len(row) > 0 else 'N/A' for row in values[:5]]
                logger.warning(f"Sample business names from sheet (column A): {sample_names}")
        
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
                # Read all invoice numbers from column F (index 5)
                # Sheet structure: A=Member, B=Solution, C=Savings Amount, D=GST, E=Total Invoice, F=Invoice Number, G=Due Date
                logger.info("Reading all invoice numbers from column F to find the highest")
                result = service.spreadsheets().values().get(
                    spreadsheetId=SHEET_ID,
                    range=f"{SHEET_NAME}!F2:F"  # Read all invoice numbers from column F
                ).execute()
                
                values = result.get('values', [])
                logger.info(f"Found {len(values)} rows with invoice numbers")
                
                # Extract numbers from all invoice numbers
                unique_invoice_numbers = set()
                for row in values:
                    if row and len(row) > 0:
                        inv_num = str(row[0]).strip()
                        if inv_num:
                            unique_invoice_numbers.add(inv_num)
                            match = re.search(r'RA(\d+)', inv_num)
                            if match:
                                num = int(match.group(1))
                                max_number = max(max_number, num)
                                logger.info(f"Found invoice number: {inv_num} -> number: {num}")
                
                logger.info(f"Unique invoice numbers found: {len(unique_invoice_numbers)}")
                logger.info(f"Highest invoice number: {max_number}")
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

