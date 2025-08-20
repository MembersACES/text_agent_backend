"""
Supplier Quote Request Tool with Email Templates
"""
import requests
from typing import Optional, Literal, List
import logging
import json
import time
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
    }
}

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
    
    # Generate subject line
    if utility_type_identifier == "C&I Electricity":
        subject = f"Quote Request - {business_name} - NMI: {nmi}"
        
        # Generate attachments list based on available files
        attachments_html = """  <p>Please see attached:</p> 
  <ul>
    <li> The Letter of Authority </li> 
    <li> Copy of recent invoice </li>"""
        
        if interval_data_file_id:
            attachments_html += """
    <li> Interval Data</li>"""
        
        attachments_html += """
  </ul>"""
        
        # Generate HTML template for C&I Electricity
        html_content = f"""<!DOCTYPE html>
<html>
<head>
  <base target="_top">
</head>
<body>
  <p>Hello Team, </p>
  <p>I hope this email finds you well. </p>
  <p>I am requesting a quote for my client {business_name} NMI: {nmi}. Can you please provide a quote with the provided information below:</p>
  <ul>
    <li>Start Date - {start_date}</li>
    <li>Contract Options - {quote_details} </li>
    <li>Commission - {commission} </li>
    <li>Current FRMP - {current_retailer} </li>
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
    <li>NMI - {nmi} </li>
  </ul>
    <p>Please Find Below the Site details:</p>
  <ul>
    <li>Yearly Peak Consumption est  - {yearly_peak_est:,} kWh </li>
    <li>Yearly Shoulder Consumption est  - {yearly_shoulder_est:,} kWh </li>
    <li>Yearly Off-Peak Consumption est  - {yearly_off_peak_est:,} kWh </li>
    <li>Total Yearly Consumption est  - {yearly_consumption_est:,} kWh </li>
  </ul>
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
    
    elif utility_type_identifier == "C&I Gas":
        subject = f"Quote Request - {business_name} - MRIN: {mrin}"
        
        # Gas typically doesn't have interval data
        html_content = f"""<!DOCTYPE html>
<html>
<head>
  <base target="_top">
</head>
<body>
  <p>Hello Team, </p>
  <p>I hope this email finds you well. </p>
  <p>I am requesting a quote for my client {business_name} MRIN: {mrin}. Can you please provide a quote with the provided information below:</p>
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
    <li>MRIN - {mrin} </li>
  </ul>
    <p>Please Find Below the Site details:</p>
  <ul>
    <li>Total Yearly Consumption est  - {yearly_consumption_est:,} MJ </li>
  </ul>
  <p>Please see attached:</p> 
  <ul>
    <li> The Letter of Authority </li> 
    <li> Copy of recent invoice </li> 
  </ul>
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
    
    elif utility_type_identifier == "SME Electricity":
        subject = f"Quote Request - {business_name} - NMI: {nmi}"
        
        # SME typically doesn't have interval data, but check anyway
        attachments_html = """  <p>Please see attached:</p> 
  <ul>
    <li> The Letter of Authority </li> 
    <li> Recent Invoice</li>"""
        
        if interval_data_file_id:
            attachments_html += """
    <li> Interval Data</li>"""
        
        attachments_html += """
  </ul>"""
        
        # Simplified template for SME
        html_content = f"""<!DOCTYPE html>
<html>
<head>
  <base target="_top">
</head>
<body>
  <p>Hello Team, </p>
  <p>I hope this email finds you well. </p>
  <p>I am requesting a quote for my client {business_name} NMI: {nmi}. Can you please provide a quote with the provided information below:</p>
  <ul>
    <li>Start Date - {start_date}</li>
    <li>Contract Options - {quote_details} </li>
    <li>Commission - {commission} </li>
    <li>Current FRMP - {current_retailer} </li>
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
    <li>NMI - {nmi} </li>
  </ul>
    <p>Please Find Below the Site details:</p>
  <ul>
    <li>Total Yearly Consumption est  - {yearly_consumption_est:,} kWh </li>
  </ul>
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
    
    elif utility_type_identifier == "SME Gas":
        subject = f"Quote Request - {business_name} - MRIN: {mrin}"
        
        html_content = f"""<!DOCTYPE html>
<html>
<head>
  <base target="_top">
</head>
<body>
  <p>Hello Team, </p>
  <p>I hope this email finds you well. </p>
  <p>I am requesting a quote for my client {business_name} MRIN: {mrin}. Can you please provide a quote with the provided information below:</p>
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
    <li>MRIN - {mrin} </li>
  </ul>
    <p>Please Find Below the Site details:</p>
  <ul>
    <li>Total Yearly Consumption est  - {yearly_consumption_est:,} MJ </li>
  </ul>
  <p>Please see attached:</p> 
  <ul>
    <li> The Letter of Authority </li> 
    <li> Recent Invoice</li> 
  </ul>
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
    
    elif utility_type_identifier == "Waste":
        subject = f"Quote Request - {business_name} - Waste Management"
        
        html_content = f"""<!DOCTYPE html>
<html>
<head>
  <base target="_top">
</head>
<body>
  <p>Hello Team, </p>
  <p>I hope this email finds you well. </p>
  <p>I am requesting a quote for my client {business_name} for waste management services. Can you please provide a quote with the provided information below:</p>
  <ul>
    <li>Start Date - {start_date}</li>
    <li>Service Options - {quote_details} </li>
    <li>Commission - {commission} </li>
    <li>Current Provider - {current_retailer} </li>
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
  </ul>
    <p>Please Find Below the Site details:</p>
  <ul>
    <li>Annual Waste Volume est  - {yearly_consumption_est:,} tonnes </li>
  </ul>
  <p>Please see attached:</p> 
  <ul>
    <li> The Letter of Authority </li> 
    <li> Recent Invoice</li> 
  </ul>
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
    
    elif utility_type_identifier == "Oil":
        subject = f"Quote Request - {business_name} - Cooking Oil Services"
        
        html_content = f"""<!DOCTYPE html>
<html>
<head>
  <base target="_top">
</head>
<body>
  <p>Hello Team, </p>
  <p>I hope this email finds you well. </p>
  <p>I am requesting a quote for my client {business_name} for cooking oil supply and collection services. Can you please provide a quote with the provided information below:</p>
  <ul>
    <li>Start Date - {start_date}</li>
    <li>Service Options - {quote_details} </li>
    <li>Commission - {commission} </li>
    <li>Current Provider - {current_retailer} </li>
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
  </ul>
    <p>Please Find Below the Site details:</p>
  <ul>
    <li>Annual Oil Volume est  - {yearly_consumption_est:,} litres </li>
  </ul>
  <p>Please see attached:</p> 
  <ul>
    <li> The Letter of Authority </li> 
    <li> Recent Invoice</li> 
  </ul>
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
    
    else:
        # Default template for other types
        subject = f"Quote Request - {business_name}"
        html_content = f"""<!DOCTYPE html>
<html>
<head>
  <base target="_top">
</head>
<body>
  <p>Hello Team, </p>
  <p>I hope this email finds you well. </p>
  <p>I am requesting a quote for my client {business_name}. Please find the details below:</p>
  <p>Please let me know if you have any questions or concerns.</p>
  <p>Kind Regards,</p>
  <p>Alice </p>
  <p>ForNRG Team </p>
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
) -> dict:
    """
    Send quote requests to multiple selected retailers via n8n webhook with email templates.
    """
    
    print("DEBUG: Function called successfully with all parameters!")
    print(f"DEBUG: utility_type_identifier = {utility_type_identifier}")
    print(f"DEBUG: retailer_type_identifier = {retailer_type_identifier}")
    
    logger.info(f"Starting quote request for business: {business_name}")
    logger.info(f"Selected retailers: {selected_retailers}")
    
    # Validate retailers
    invalid_retailers = [r for r in selected_retailers if r not in QUOTE_RETAILER_EMAILS]
    if invalid_retailers:
        raise ValueError(f"Invalid retailers: {invalid_retailers}")
    
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
    
    # Add retailer-specific information
    email_drafts_created = []
    for retailer in selected_retailers:
        retailer_info = QUOTE_RETAILER_EMAILS[retailer]
        n8n_payload["retailers"].append({
            "name": retailer_info["name"],
            "email": retailer_info["email"],
            "retailer_type": "C&I" if "C&I" in retailer else "SME"
        })
        
        # Track email drafts created
        email_drafts_created.append({
            "retailer": retailer_info["name"],
            "subject": email_template["subject"],
            "to": retailer_info["email"]
        })
    
    # Send to n8n webhook
    try:
        logger.info(f"Sending quote request to n8n webhook for {len(selected_retailers)} retailers")
        
        response = requests.post(
            "https://membersaces.app.n8n.cloud/webhook/supplier-quote-request",
            json=n8n_payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if response.status_code == 200:
            logger.info("Quote request sent successfully to n8n")
            return {
                "success": True,
                "quote_request_id": f"QR_{int(time.time())}",
                "message": f"Quote request sent to {len(selected_retailers)} retailers",
                "suppliers_contacted": selected_retailers,
                "estimated_response_time": "3-5 business days",
                "email_drafts_created": email_drafts_created,
                "email_subject": email_template["subject"],
                "n8n_response": response.json() if response.content else None
            }
        else:
            logger.error(f"n8n webhook failed with status {response.status_code}: {response.text}")
            raise Exception(f"n8n webhook failed: {response.status_code}")
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send quote request to n8n: {e}")
        raise Exception(f"Failed to send quote request: {e}")
    
    except Exception as e:
        logger.error(f"Unexpected error in quote request: {e}")
        raise