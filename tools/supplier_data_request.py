"""
Supplier Data Request Tool with Email Templates
"""
import requests
from typing import Optional, Literal
import logging
from tools.business_info import get_business_information
import re
import json

logger = logging.getLogger(__name__)

# Retailer email mapping with fuzzy matching support
RETAILER_EMAILS = {
    "Origin C&I Electricity": {
        "name": "Origin C&I",
        "email": "BusinessCustomers@originenergy.com.au, data.quote@fornrg.com",
        "variants": ["Origin", "OriginEnergy"]
    },
    "BlueNRG SME Electricity": {
        "name": "BlueNRG SME",
        "email": "info@bluenrg.com.au, data.quote@fornrg.com",
        "variants": ["Blue NRG Pty Ltd", "BlueNRG", "Blue NRG"]
    },
    "Origin C&I Gas": {
        "name": "Origin C&I",
        "email": "BusinessCustomers@originenergy.com.au, data.quote@fornrg.com",
        "variants": ["Origin"]
    },
    "Momentum C&I Electricity": {
        "name": "Momentum",
        "email": "CIOperations@hydro.com.au, data.quote@fornrg.com",
        "variants": ["Momentum Energy Pty Ltd", "Momentum Energy"]
    },
    "Shell Energy": {
        "name": "Shell",
        "email": "retailadmin@shellenergy.com.au, data.quote@fornrg.com",
        "variants": ["Shell Energy Retail Pty Ltd", "Shell Energy", "Shell"]
    },
    "CovaU SME Electricity": {
        "name": "CovaU",
        "email": "corp.sales@covau.com.au, data.quote@fornrg.com",
        "variants": ["CovaU", "CovaU SME"]
    },
    "Veolia Waste": {
        "name": "Veolia",
        "email": "cx.service@veolia.com.au, business@acesolutions.com.au",
        "variants": ["Veolia"]
    },
    "Alinta C&I Electricity": {
        "name": "Alinta",
        "email": "ci.services@alintaenergy.com.au, data.quote@fornrg.com",
        "variants": ["Alinta Energy", "alintaenergy", "Alinta"]
    },
    "Alinta C&I Gas": {
        "name": "Alinta",
        "email": "ci.services@alintaenergy.com.au, data.quote@fornrg.com",
        "variants": ["Alinta Energy", "alintaenergy", "Alinta"]
    },
    "Energy Australia C&I E & G": {
        "name": "Energy Australia",
        "email": "businessenq@energyaustralia.com.au, data.quote@fornrg.com",
        "variants": ["EnergyAustralia", "Energy Australia"]
    },
    "AGL C&I E & G": {
        "name": "AGL",
        "email": "businesscustomers@agl.com.au, data.quote@fornrg.com",
        "variants": ["AGL Sales Pty Limited", "AGL", "AGL Sales"]
    },
    "Alinta C&I Electricity & Gas": {
        "name": "Alinta",
        "email": "ci.services@alintaenergy.com.au, data.quote@fornrg.com",
        "variants": ["Alinta Energy", "alintaenergy", "Alinta"]
    },
    "Next Business Energy SME": {
        "name": "Next Business Energy",
        "email": "info@nextbusinessenergy.com.au, data.quote@fornrg.com",
        "variants": ["Next Business Energy Pty Ltd", "Next Business Energy"]
    },
    "1st Energy SME": {
        "name": "1st Energy",
        "email": "support@1stenergy.com.au, data.quote@fornrg.com",
        "variants": ["1st Energy"]
    },
    "Origin SME": {
        "name": "Origin SME",
        "email": "BusinessCentre@originenergy.com.au, data.quote@fornrg.com",
        "variants": ["Origin"]
    },
    "CovaU SME": {
        "name": "CovaU SME",
        "email": "support@covau.com.au, data.quote@fornrg.com",
        "variants": ["CovaU", "CovaU SME"]
    },
    "Red Energy SME": {
        "name": "Red Energy",
        "email": "business@redenergy.com.au, data.quote@fornrg.com",
        "variants": ["Red Energy Pty Ltd", "RED ENERGY PTY. LIMITED", "Red Energy"]
    },
    "GloBird Energy SME": {
        "name": "GloBird",
        "email": "customerservice@globirdenergy.com.au, data.quote@fornrg.com",
        "variants": ["GloBird Energy", "GloBird"]
    },
    "Powerdirect SME": {
        "name": "Powerdirect",
        "email": "powerdirectservice@powerdirect.com.au, data.quote@fornrg.com",
        "variants": ["Powerdirect"]
    },
    "Sumo SME": {
        "name": "Sumo",
        "email": "info@sumo.com.au, data.quote@fornrg.com",
        "variants": ["Sumo"]
    },
    "Momentum SME": {
        "name": "Momentum SME",
        "email": "info@momentum.com.au, data.quote@fornrg.com",
        "variants": ["Momentum Energy Pty Ltd", "Momentum Energy"]
    },
    "Tango Energy": {
        "name": "Tango Energy",
        "email": "support@tangoenergy.com, data.quote@fornrg.com",
        "variants": ["Tango Energy Pty Ltd", "Tango Energy", "Tango"]
    },
    "Sun Retail": {
        "name": "Sun Retail",
        "email": "support@sunretail.com.au, data.quote@fornrg.com",
        "variants": ["Sun Retail Pty Ltd", "Sun Retail"]
    },
    "Ergon Energy": {
        "name": "Ergon Energy",
        "email": "commercial@ergon.com.au, data.quote@fornrg.com",
        "variants": ["Ergon Energy Queensland Pty Ltd", "Ergon Energy"]
    },
    "Other": {
        "name": "Other Supplier",
        "email": "members@acesolutions.com.au, data.quote@fornrg.com",
        "variants": []
    }
}

def find_retailer_email(supplier_name: str, service_type: str) -> tuple[str, str, bool]:
    """
    Find the retailer email address based on supplier name with fuzzy matching
    
    Args:
        supplier_name: Name of the supplier (may be in various formats)
        service_type: Type of service to determine which variant to use
        
    Returns:
        Tuple of (email_address, resolved_name, is_default)
    """
    supplier_name = supplier_name.strip()
    logger.info(f"Looking up email for supplier: '{supplier_name}', service: '{service_type}'")
    
    # Normalize the supplier name for matching
    normalized_supplier = supplier_name.lower().replace("pty ltd", "").replace("pty. limited", "").replace("(vic)", "").strip()
    
    # First, try exact matches
    for key, config in RETAILER_EMAILS.items():
        if supplier_name.lower() == config["name"].lower():
            logger.info(f"Exact match found: {key}")
            return config["email"], config["name"], False
        
        # Check variants
        for variant in config.get("variants", []):
            if supplier_name.lower() == variant.lower():
                logger.info(f"Variant match found: {key}")
                return config["email"], config["name"], False
    
    # Second, try special handling for known retailers with service type logic
    if "origin" in normalized_supplier or "origin energy" in normalized_supplier:
        logger.info(f"Origin detected - checking service type: {service_type}")
        if service_type in ["electricity_ci", "gas_ci"]:
            return RETAILER_EMAILS["Origin C&I Electricity"]["email"], "Origin C&I", False
        elif service_type in ["electricity_sme", "gas_sme"]:
            return RETAILER_EMAILS["Origin SME"]["email"], "Origin SME", False
    
    if "momentum" in normalized_supplier or "momentum energy" in normalized_supplier:
        logger.info(f"Momentum detected - checking service type: {service_type}")
        if service_type in ["electricity_ci", "gas_ci"]:
            return RETAILER_EMAILS["Momentum C&I Electricity"]["email"], "Momentum C&I", False
        elif service_type in ["electricity_sme", "gas_sme"]:
            return RETAILER_EMAILS["Momentum SME"]["email"], "Momentum SME", False
    
    if "agl" in normalized_supplier:
        return RETAILER_EMAILS["AGL C&I E & G"]["email"], "AGL", False
    
    if "shell" in normalized_supplier:
        return RETAILER_EMAILS["Shell Energy"]["email"], "Shell Energy", False
    
    if "alinta" in normalized_supplier:
        if service_type in ["electricity_ci", "gas_ci"]:
            return RETAILER_EMAILS["Alinta C&I Electricity & Gas"]["email"], "Alinta C&I", False
        # Add SME handling for Alinta if needed
    
    if "energyaustralia" in normalized_supplier or "energy australia" in normalized_supplier:
        return RETAILER_EMAILS["Energy Australia C&I E & G"]["email"], "Energy Australia", False
    
    # Third, try partial matches (but this should come after the special handling)
    for key, config in RETAILER_EMAILS.items():
        if config["name"].lower() in normalized_supplier:
            logger.info(f"Partial match found: {key} (but already handled in special cases)")
            continue  # Skip if already handled above
    
    # Fourth, check variants with fuzzy matching
    for key, config in RETAILER_EMAILS.items():
        for variant in config.get("variants", []):
            if variant.lower() in normalized_supplier or normalized_supplier in variant.lower():
                logger.info(f"Fuzzy variant match found: {key}")
                return config["email"], config["name"], False
    
    # Default to Other if no match found
    logger.warning(f"No match found for supplier: '{supplier_name}', using default email")
    return RETAILER_EMAILS["Other"]["email"], f"{supplier_name} (Unknown Retailer)", True


EMAIL_TEMPLATES = {
    "electricity_ci_data": {
        "subject": "C&I Electricity Data Request - {business_name} - NMI: {nmi}",
        "template": """<p>Hello,</p>
<p>I hope this email finds you well.</p>
<p>I am requesting the following information for my client {business_name} NMI: {nmi}.</p>
<p>Can you please provide the below information:</p>
<ul>
<li>12 Months Interval Data</li>
<li>Contract End Date</li>
<li>Direct Metering Agreement End Date</li>
<li>Copy of the most recent invoice</li>
</ul>
<p>Please see attached:</p>
<ul>
<li>The Letter of Authority</li>
</ul>
<p>Please let me know if you have any questions or concerns.</p>
<p>Kind Regards,</p>
<p>Alice</p>
<p>ForNRG Team</p>
<p>FORNRG Pty Ltd</p>
<p>P: 1300 440 224</p>
<p>W: <a href="http://www.fornrg.com/">http://www.fornrg.com/</a></p>
<p>NOTE: This email, including any attachments, is strictly confidential. If you received this email in error, please notify the sender and delete it as well as any copies from your system. You must not use, print, distribute, copy, or disclose the content of this email if you are not the intended recipient.</p>"""
    },
    "electricity_sme_data": {
        "subject": "SME Electricity Data Request - {business_name} - NMI: {nmi}",
        "template": """<p>Hello,</p>
<p>I hope this email finds you well.</p>
<p>I am requesting the following information for my client {business_name} NMI: {nmi}. Can you please provide the below information:</p>
<ul>
<li>End date of contract (if applicable)</li>
<li>12 Months worth of invoices (if applicable)</li>
</ul>
<p>Please see attached:</p>
<ul>
<li>The Letter of Authority</li>
</ul>
<p>Please let me know if you have any questions or concerns.</p>
<p>Kind Regards,</p>
<p>Alice</p>
<p>ForNRG Team</p>
<p>FORNRG Pty Ltd</p>
<p>P: 1300 440 224</p>
<p>W: <a href="http://www.fornrg.com/">http://www.fornrg.com/</a></p>
<p>NOTE: This email, including any attachments, is strictly confidential.</p>"""
    },
    "gas_ci_data": {
        "subject": "C&I Gas Data Request - {business_name} - MRIN: {mrin}",
        "template": """<p>Hello,</p>
<p>I hope this email finds you well.</p>
<p>I am requesting the following information for my client {business_name} MRIN: {mrin}. Can you please provide the below information:</p>
<ul>
<li>12 Months Interval Data</li>
<li>Contract End Date</li>
<li>Copy of 12 months worth of invoices</li>
<li>Copy of current contract in place</li>
</ul>
<p>Please see attached:</p>
<ul>
<li>The Letter of Authority</li>
</ul>
<p>Please let me know if you have any questions or concerns.</p>
<p>Kind Regards,</p>
<p>Alice</p>
<p>ForNRG Team</p>
<p>FORNRG Pty Ltd</p>
<p>P: 1300 440 224</p>
<p>W: <a href="http://www.fornrg.com/">http://www.fornrg.com/</a></p>"""
    },
    "gas_sme_data": {
        "subject": "SME Gas Data Request - {business_name} - MRIN: {mrin}",
        "template": """<p>Hello,</p>
<p>I hope this email finds you well.</p>
<p>I am requesting the following information for my client {business_name} MRIN: {mrin}. Can you please provide the below information:</p>
<ul>
<li>End date of contract (if applicable)</li>
<li>12 Months worth of invoices (if applicable)</li>
</ul>
<p>Please see attached:</p>
<ul>
<li>The Letter of Authority</li>
</ul>
<p>Please let me know if you have any questions or concerns.</p>
<p>Kind Regards,</p>
<p>Alice</p>
<p>ForNRG Team</p>"""
    },
    "waste_data": {
        "subject": "Waste Data Request - {business_name} - Account Number: {account_number}",
        "template": """<p>Hi Team,</p>
<p>I hope this email finds you well.</p>
<p>I am contacting you on behalf of our member {business_name} Account Number (if applicable): {account_number}.</p>
<p>{business_name} has engaged our services to conduct its GHG emissions report. Please see the letter of authority attached & a recent invoice (if available).</p>
<p>Can you please send me the below details for a 12 month period (preferable most recent)</p>
<ul>
<li>12 Months worth of invoices</li>
<li>Bin Lift weights per waste stream</li>
<li>Copy of current contract</li>
<li>For all recycled waste please confirm that it has been recycled. If some of the recycled products have gone to landfill please provide the quantity or % of that has gone to landfill.</li>
</ul>
<p>Please let me know if you have any questions or concerns.</p>
<p>Kind Regards,</p>
<p>Sofie</p>
<p><b>Australian Circular Economy Solution</b></p>
<p>470 St Kilda Road, Melbourne VIC 3004</p>
<p>Ph: 1300 849 908 | Website: acesolutions.com.au</p>"""
    },
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


def supplier_data_request(
    supplier_name: str,
    business_name: str,
    service_type: Literal["electricity_ci", "electricity_sme", "gas_ci", "gas_sme", "waste"],
    account_identifier: str,
    identifier_type: str = "NMI"
) -> str:
    """
    Request data from a supplier using specific email templates and attaching LOA
   
    Args:
        supplier_name: Name of the supplier
        business_name: Name of the business
        service_type: Type of service (electricity_ci, electricity_sme, gas_ci, gas_sme, waste)
        account_identifier: The account identifier (NMI, MRIN, or account number)
        identifier_type: Type of identifier (NMI, MRIN, or account_number)
    """
    
    try:
        # Clean input parameters - remove any extra brackets or special characters
        supplier_name = clean_parameter(supplier_name)
        business_name = clean_parameter(business_name)
        account_identifier = clean_parameter(account_identifier)
        
        # Create formatted service type for display
        service_type_display = ""
        if service_type == "electricity_ci":
            service_type_display = "C&I Electricity"
        elif service_type == "electricity_sme":
            service_type_display = "SME Electricity"  
        elif service_type == "gas_ci":
            service_type_display = "C&I Gas"
        elif service_type == "gas_sme":
            service_type_display = "SME Gas"
        elif service_type == "waste":
            service_type_display = "Waste"
        
        logger.info(f"========== SUPPLIER DATA REQUEST ==========")
        logger.info(f"Raw inputs - Supplier: '{supplier_name}'")
        logger.info(f"Raw inputs - Business: '{business_name}'")
        logger.info(f"Raw inputs - Account: '{account_identifier}'")
        logger.info(f"Service type: {service_type}")
        logger.info(f"Identifier type: {identifier_type}")
        logger.info(f"========================================")
        
        # Get template
        template_key = f"{service_type}_data"
        if template_key not in EMAIL_TEMPLATES:
            logger.error(f"No template found for service type: {service_type}")
            return f"Error: No template found for {service_type}. Valid types are: electricity_ci, electricity_sme, gas_ci, gas_sme, waste"
       
        template_config = EMAIL_TEMPLATES[template_key]
        
        # Look up supplier email
        supplier_email, resolved_supplier_name, is_default = find_retailer_email(supplier_name, service_type)
        logger.info(f"Resolved supplier email: {supplier_email} for {resolved_supplier_name} (default: {is_default})")
        
        # If using default email, add a warning note
        default_note = ""
        if is_default:
            default_note = f"\n\n⚠️ Note: '{supplier_name}' was not recognized in our retailer database. The request will be sent to our general members email address for manual processing."
        
        # Prepare template variables
        template_vars = {
            "business_name": business_name,
            "nmi": account_identifier if identifier_type == "NMI" else "",
            "mrin": account_identifier if identifier_type == "MRIN" else "",
            "account_number": account_identifier if identifier_type == "account_number" else ""
        }
       
        # Format email - catch any formatting errors
        try:
            email_body = template_config["template"].format(**template_vars)
            email_subject = template_config["subject"].format(**template_vars)
        except KeyError as e:
            logger.error(f"Template formatting error: {e}")
            return f"Error: Template formatting failed - missing variable: {e}"
        
        # Prepare request for n8n
        request_data = {
            "supplier_name": resolved_supplier_name,
            "supplier_email": supplier_email,
            "original_supplier_name": supplier_name,
            "business_name": business_name,
            "service_type": service_type,
            "account_identifier": account_identifier,
            "identifier_type": identifier_type,
            "email_subject": email_subject,
            "email_body": email_body,
            "request_type": "supplier_data_request"
        }
        
        logger.info(f"Sending to n8n webhook...")
        logger.debug(f"Request data: {json.dumps(request_data, indent=2)}")
        
        response = requests.post(
            "https://membersaces.app.n8n.cloud/webhook/supplier-data-request",
            json=request_data,
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
                
                # Check multiple conditions for success
                if result.get("status") == "success" or result.get("success") == True:
                    return f"""✅ Data request successfully sent to {resolved_supplier_name}:

📧 Request Details:
- Request ID: {result.get('request_id', 'N/A')}
- Business: {business_name}
- Service Type: {service_type_display}
- Account: {identifier_type} - {account_identifier}
- LOA File ID: {result.get('loa_file_id', 'N/A')}

📨 Email Information:
- Subject: {email_subject}
- Sent to: {supplier_email}
- Supplier: {resolved_supplier_name} (from: {supplier_name})
{default_note}"""
                else:
                    # Log the actual response to understand the issue
                    logger.error(f"n8n response doesn't indicate success: {result}")
                    
                    # Check if it's a known error pattern
                    if "error" in result:
                        error_message = result.get("error", "Unknown error")
                        if "LOA not found" in error_message:
                            return f"❌ Error: Letter of Authority not found for {business_name}. Please ensure the LOA is uploaded first."
                        elif "supplier email not found" in error_message:
                            return f"❌ Error: Email address not found for supplier {supplier_name}. Please check the supplier name."
                        else:
                            return f"❌ Error from n8n: {error_message}"
                    
                    # If email was actually sent but response format is different
                    if any(key in result for key in ["emailSent", "email_sent", "sent"]):
                        return f"""✅ Data request sent to {resolved_supplier_name}:
- Business: {business_name}
- Service Type: {service_type_display}
- Account: {identifier_type} - {account_identifier}
- Email: {supplier_email}
{default_note}

Note: Email sent but response format was unexpected."""
                    
                    return f"❌ Unexpected response from n8n: {result}"
                    
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON: {e}")
                # Maybe n8n is returning success differently
                if "success" in response.text.lower() or "sent" in response.text.lower():
                    return f"""✅ Data request sent to {resolved_supplier_name}:
- Business: {business_name}
- Service Type: {service_type_display}
- Account: {identifier_type} - {account_identifier}
- Email: {supplier_email}
{default_note}

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
__all__ = ['supplier_data_request']