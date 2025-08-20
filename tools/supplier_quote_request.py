"""
Supplier Quote Request Tool with Email Templates
"""
import requests
from typing import Optional, Literal, List
import logging
import json
import time
import re
from datetime import datetime

logger = logging.getLogger(__name__)

# Retailer email mapping for quote requests
QUOTE_RETAILER_EMAILS = {
    "Test": {
        "name": "Test",
        "email": "data.quote@fornrg.com",
        "variants": ["Test"]
    },
    "Origin C&I": {
        "name": "Origin C&I",
        "email": "BusinessCustomers@originenergy.com.au, data.quote@fornrg.com",
        "variants": ["Origin", "OriginEnergy"]
    },
    "Alinta C&I": {
        "name": "Alinta C&I",
        "email": "ci.services@alintaenergy.com.au, data.quote@fornrg.com",
        "variants": ["Alinta Energy", "alintaenergy", "Alinta"]
    },
    "Shell C&I": {
        "name": "Shell C&I",
        "email": "retailadmin@shellenergy.com.au, data.quote@fornrg.com",
        "variants": ["Shell Energy Retail Pty Ltd", "Shell Energy", "Shell"]
    },
    "Momentum C&I": {
        "name": "Momentum C&I",
        "email": "CIOperations@hydro.com.au, data.quote@fornrg.com",
        "variants": ["Momentum Energy Pty Ltd", "Momentum Energy"]
    },
    "Origin SME": {
        "name": "Origin SME",
        "email": "BusinessCustomers@originenergy.com.au, data.quote@fornrg.com",
        "variants": ["Origin", "OriginEnergy"]
    },
    "Alinta SME": {
        "name": "Alinta SME",
        "email": "ci.services@alintaenergy.com.au, data.quote@fornrg.com",
        "variants": ["Alinta Energy", "alintaenergy", "Alinta"]
    },
    "Shell SME": {
        "name": "Shell SME",
        "email": "retailadmin@shellenergy.com.au, data.quote@fornrg.com",
        "variants": ["Shell Energy Retail Pty Ltd", "Shell Energy", "Shell"]
    },
    "Momentum SME": {
        "name": "Momentum SME",
        "email": "CIOperations@hydro.com.au, data.quote@fornrg.com",
        "variants": ["Momentum Energy Pty Ltd", "Momentum Energy"]
    },
    "Waste Provider 1": {
        "name": "Waste Provider 1",
        "email": "data.quote@fornrg.com",
        "variants": ["Waste Provider 1"]
    },
    "Waste Provider 2": {
        "name": "Waste Provider 2",
        "email": "data.quote@fornrg.com",
        "variants": ["Waste Provider 2"]
    },
    "Oil Provider 1": {
        "name": "Oil Provider 1",
        "email": "data.quote@fornrg.com",
        "variants": ["Oil Provider 1"]
    },
    "Oil Provider 2": {
        "name": "Oil Provider 2",
        "email": "data.quote@fornrg.com",
        "variants": ["Oil Provider 2"]
    }
}

def clean_parameter(value: str) -> str:
    """Clean parameter values to remove extra brackets and whitespace"""
    if not value:
        return value
    
    # Remove all types of brackets and quotes
    cleaned = value.strip()
    cleaned = re.sub(r'[\[\](){}<>]+', '', cleaned)  # All types of brackets
    cleaned = re.sub(r'["\']', '', cleaned)  # Quotes
    cleaned = re.sub(r'\s+', ' ', cleaned)  # Multiple spaces to single space
    cleaned = cleaned.strip()
    
    logger.info(f"Cleaned parameter: '{value}' -> '{cleaned}'")
    return cleaned

def generate_email_template(
    utility_type_identifier: str,
    business_name: str,
    nmi: Optional[str] = None,
    mrin: Optional[str] = None,
    start_date: str = "",
    quote_details: str = "",
    commission: str = "",
    current_retailer: str = "",
    offer_due: str = "",
    trading_as: str = "",
    abn: str = "",
    site_address: str = "",
    client_name: str = "",
    client_number: str = "",
    client_email: str = "",
    yearly_peak_est: int = 0,
    yearly_shoulder_est: int = 0,
    yearly_off_peak_est: int = 0,
    yearly_consumption_est: int = 0,
    interval_data_file_id: Optional[str] = None
) -> dict:
    """Generate email subject and HTML content based on utility type"""
    
    # Generate subject line based on utility type
    if utility_type_identifier == "C&I Electricity":
        subject = f"Quote Request - {business_name} - NMI: {nmi}"
        identifier_display = f"NMI - {nmi}"
        consumption_unit = "kWh"
    elif utility_type_identifier == "C&I Gas":
        subject = f"Quote Request - {business_name} - MRIN: {mrin}"
        identifier_display = f"MRIN - {mrin}"
        consumption_unit = "MJ"
    elif utility_type_identifier == "SME Electricity":
        subject = f"Quote Request - {business_name} - NMI: {nmi}"
        identifier_display = f"NMI - {nmi}"
        consumption_unit = "kWh"
    elif utility_type_identifier == "SME Gas":
        subject = f"Quote Request - {business_name} - MRIN: {mrin}"
        identifier_display = f"MRIN - {mrin}"
        consumption_unit = "MJ"
    elif utility_type_identifier == "Waste":
        subject = f"Quote Request - {business_name} - Waste Management"
        identifier_display = "Waste Services"
        consumption_unit = "tonnes"
    elif utility_type_identifier == "Oil":
        subject = f"Quote Request - {business_name} - Cooking Oil Services"
        identifier_display = "Oil Services"
        consumption_unit = "litres"
    else:
        subject = f"Quote Request - {business_name}"
        identifier_display = "Services"
        consumption_unit = "units"
    
    # Generate attachments section
    attachments_html = """  <p>Please see attached:</p> 
  <ul>
    <li> The Letter of Authority </li> 
    <li> Copy of recent invoice </li>"""
    
    if interval_data_file_id and ("Electricity" in utility_type_identifier):
        attachments_html += """
    <li> Interval Data</li>"""
    
    attachments_html += """
  </ul>"""
    
    # Generate consumption details based on utility type
    if utility_type_identifier in ["C&I Electricity", "SME Electricity"] and yearly_peak_est > 0:
        consumption_html = f"""    <p>Please Find Below the Site details:</p>
  <ul>
    <li>Yearly Peak Consumption est  - {yearly_peak_est:,} {consumption_unit} </li>
    <li>Yearly Shoulder Consumption est  - {yearly_shoulder_est:,} {consumption_unit} </li>
    <li>Yearly Off-Peak Consumption est  - {yearly_off_peak_est:,} {consumption_unit} </li>
    <li>Total Yearly Consumption est  - {yearly_consumption_est:,} {consumption_unit} </li>
  </ul>"""
    else:
        consumption_html = f"""    <p>Please Find Below the Site details:</p>
  <ul>
    <li>Total Yearly Consumption est  - {yearly_consumption_est:,} {consumption_unit} </li>
  </ul>"""
    
    # Generate HTML template
    html_content = f"""<!DOCTYPE html>
<html>
<head>
  <base target="_top">
</head>
<body>
  <p>Hello Team, </p>
  <p>I hope this email finds you well. </p>
  <p>I am requesting a quote for my client {business_name} {identifier_display}. Can you please provide a quote with the provided information below:</p>
  <ul>
    <li>Start Date - {start_date}</li>
    <li>Contract Options - {quote_details} </li>
    <li>Commission - {commission} </li>
    <li>Current Retailer - {current_retailer} </li>
    <li>Offer Due - {offer_due}</li>
  </ul>
    <p>Please Find Below the member's details:</p>
  <ul>
    <li>Company Name  - {business_name} </li>
    <li>Trading as -  {trading_as} </li>
    <li>ABN - {abn} </li>
    <li>Site Address - {site_address} </li>
    <li>Contact Name - {client_name} </li>
    <li>Number - {client_number} </li>
    <li>Email Address - {client_email} </li>
    <li>{identifier_display} </li>
  </ul>
{consumption_html}
{attachments_html}
  <p>Please let me know if you have any questions or concerns.</p>
  <p>Kind Regards,</p>
  <p>Alice </p>
  <p>ForNRG Team </p>
  <p> FORNRG Pty Ltd </p>
  <p> P: 1300 440 224 </p>
  <p> W: http://www.fornrg.com/ </p>
  <p> NOTE: This email, including any attachments, is strictly confidential. If you received this email in error, please notify the sender and delete it as well as any copies from your system. You must not use, print, distribute, copy, or disclose the content of this email if you are not the intended recipient. </p>
</body>
</html>"""
    
    return {
        "subject": subject,
        "html_content": html_content
    }

def send_supplier_quote_request(
    selected_retailers: List[str],
    business_name: str,
    nmi: Optional[str] = None,
    mrin: Optional[str] = None,
    utility_type: str = "",
    utility_type_identifier: str = "",
    retailer_type_identifier: str = "",
    quote_type: str = "",
    quote_details: str = "",
    commission: str = "0",
    start_date: str = "",
    offer_due: str = "",
    yearly_peak_est: int = 0,
    yearly_shoulder_est: int = 0,
    yearly_off_peak_est: int = 0,
    yearly_consumption_est: int = 0,
    current_retailer: str = "",
    trading_as: str = "",
    abn: str = "",
    site_address: str = "",
    client_name: str = "",
    client_number: str = "",
    client_email: str = "",
    loa_file_id: Optional[str] = None,
    invoice_file_id: Optional[str] = None,
    interval_data_file_id: Optional[str] = None,
    user_email: Optional[str] = None
) -> str:
    """
    Send quote requests to multiple selected retailers via n8n webhook with email templates.
    Returns a formatted success message string similar to the data request tool.
    """
    
    try:
        # Clean input parameters - remove any extra brackets or special characters
        business_name = clean_parameter(business_name)
        current_retailer = clean_parameter(current_retailer)
        
        logger.info(f"========== SUPPLIER QUOTE REQUEST ==========")
        logger.info(f"Business: '{business_name}'")
        logger.info(f"Selected retailers: {selected_retailers}")
        logger.info(f"Utility type: {utility_type}")
        logger.info(f"Account: {nmi or mrin}")
        logger.info(f"==========================================")
        
        # Validate retailers
        invalid_retailers = [r for r in selected_retailers if r not in QUOTE_RETAILER_EMAILS]
        if invalid_retailers:
            logger.error(f"Invalid retailers: {invalid_retailers}")
            return f"❌ Error: Invalid retailers specified: {', '.join(invalid_retailers)}. Valid options are: {', '.join(QUOTE_RETAILER_EMAILS.keys())}"
        
        # Generate email template
        email_template = generate_email_template(
            utility_type_identifier=utility_type_identifier,
            business_name=business_name,
            nmi=nmi,
            mrin=mrin,
            start_date=start_date,
            quote_details=quote_details,
            commission=commission,
            current_retailer=current_retailer,
            offer_due=offer_due,
            trading_as=trading_as,
            abn=abn,
            site_address=site_address,
            client_name=client_name,
            client_number=client_number,
            client_email=client_email,
            yearly_peak_est=yearly_peak_est,
            yearly_shoulder_est=yearly_shoulder_est,
            yearly_off_peak_est=yearly_off_peak_est,
            yearly_consumption_est=yearly_consumption_est,
            interval_data_file_id=interval_data_file_id
        )
        
        # Prepare the main payload for n8n
        n8n_payload = {
            "business_name": business_name,
            "nmi": nmi,
            "mrin": mrin,
            "utility_type": utility_type,
            "utility_type_identifier": utility_type_identifier,
            "retailer_type_identifier": retailer_type_identifier,
            "quote_type": quote_type,
            "quote_details": quote_details,
            "commission": commission,
            "start_date": start_date,
            "offer_due": offer_due,
            "yearly_peak_est": yearly_peak_est,
            "yearly_shoulder_est": yearly_shoulder_est,
            "yearly_off_peak_est": yearly_off_peak_est,
            "yearly_consumption_est": yearly_consumption_est,
            "current_retailer": current_retailer,
            "trading_as": trading_as,
            "abn": abn,
            "site_address": site_address,
            "client_name": client_name,
            "client_number": client_number,
            "client_email": client_email,
            "loa_file_id": loa_file_id,
            "invoice_file_id": invoice_file_id,
            "interval_data_file_id": interval_data_file_id,
            "user_email": user_email,
            "timestamp": datetime.now().isoformat(),
            "email_subject": email_template["subject"],
            "email_html_content": email_template["html_content"],
            "retailers": []
        }
        
        # Add retailer-specific information and build email list
        retailer_emails = []
        for retailer in selected_retailers:
            retailer_info = QUOTE_RETAILER_EMAILS[retailer]
            n8n_payload["retailers"].append({
                "name": retailer_info["name"],
                "email": retailer_info["email"],
                "retailer_type": "C&I" if "C&I" in retailer else "SME"
            })
            retailer_emails.append(retailer_info["email"])
        
        logger.info(f"Sending quote request to n8n webhook...")
        logger.debug(f"Request data: {json.dumps(n8n_payload, indent=2)}")
        
        # Send to n8n webhook
        response = requests.post(
            "https://membersaces.app.n8n.cloud/webhook/supplier-quote-request",
            json=n8n_payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        logger.info(f"Response status: {response.status_code}")
        logger.info(f"Response text: {response.text[:1000]}")
        
        if response.status_code == 200:
            # Check if response is empty or not JSON
            if not response.text:
                logger.warning("Empty response from n8n")
                return "❌ Error: Empty response from n8n webhook"
                
            try:
                result = response.json()
                logger.info(f"Parsed response: {result}")
                
                # Generate quote request ID
                quote_request_id = result.get('quote_request_id', f"QR_{int(time.time())}")
                
                # Check multiple conditions for success
                if result.get("status") == "success" or result.get("success") == True:
                    return f"""✅ Quote request successfully sent to {len(selected_retailers)} retailers:

📋 Quote Request Details:
- Request ID: {quote_request_id}
- Business: {business_name}
- Service Type: {utility_type_identifier}
- Account: {nmi or mrin or 'N/A'}
- Quote Type: {quote_details}
- Start Date: {start_date}
- Commission: {commission}

📨 Email Information:
- Subject: {email_template["subject"]}
- Retailers: {', '.join(selected_retailers)}
- LOA File ID: {loa_file_id or 'N/A'}
- Invoice File ID: {invoice_file_id or 'N/A'}
- Interval Data File ID: {interval_data_file_id or 'N/A'}"""
                else:
                    # Log the actual response to understand the issue
                    logger.error(f"n8n response doesn't indicate success: {result}")
                    
                    # Check if it's a known error pattern
                    if "error" in result:
                        error_message = result.get("error", "Unknown error")
                        if "LOA not found" in error_message:
                            return f"❌ Error: Letter of Authority not found for {business_name}. Please ensure the LOA is uploaded first."
                        elif "retailer email not found" in error_message:
                            return f"❌ Error: Email address not found for one or more retailers. Please check the retailer names."
                        else:
                            return f"❌ Error from n8n: {error_message}"
                    
                    # If email was actually sent but response format is different
                    if any(key in result for key in ["emailSent", "email_sent", "sent"]):
                        return f"""✅ Quote request sent to {len(selected_retailers)} retailers:
- Business: {business_name}
- Service Type: {utility_type_identifier}
- Account: {nmi or mrin or 'N/A'}
- Retailers: {', '.join(selected_retailers)}

Note: Email sent but response format was unexpected."""
                    
                    return f"❌ Unexpected response from n8n: {result}"
                    
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON: {e}")
                # Maybe n8n is returning success differently
                if "success" in response.text.lower() or "sent" in response.text.lower():
                    return f"""✅ Quote request sent to {len(selected_retailers)} retailers:
- Business: {business_name}
- Service Type: {utility_type_identifier}
- Account: {nmi or mrin or 'N/A'}
- Retailers: {', '.join(selected_retailers)}

Note: Non-standard response format."""
                else:
                    return f"❌ Error: Invalid response format from n8n: {response.text[:500]}"
        else:
            logger.error(f"HTTP error {response.status_code}")
            logger.error(f"Response content: {response.text}")
            
            # Provide user-friendly error messages based on status code
            if response.status_code == 404:
                return "❌ Error: Webhook endpoint not found. Please contact support."
            elif response.status_code == 401:
                return "❌ Error: Authentication failed. Please check API credentials."
            elif response.status_code == 500:
                return f"❌ Error: Internal server error in n8n. Please try again later. Details: {response.text[:200]}"
            else:
                return f"❌ Error: HTTP {response.status_code} - {response.text[:500]}"
           
    except requests.exceptions.Timeout:
        logger.error("Request timeout after 30 seconds")
        return "❌ Error: Request timed out after 30 seconds. The n8n server may be busy. Please try again."
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error: {str(e)}")
        return "❌ Error: Could not connect to n8n server. Please check your internet connection."
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {str(e)}")
        return f"❌ Error: Network issue - {str(e)}"
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return f"❌ Error: Unexpected error occurred - {str(e)}"


# Export the function
__all__ = ['send_supplier_quote_request']