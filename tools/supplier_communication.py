from langchain_core.tools import tool
import requests
import logging
import re
from .drive_filing import drive_filing
from .get_business_information import get_business_information

logger = logging.getLogger(__name__)

# Contract email mappings
CONTRACT_EMAIL_MAPPINGS = {
    "PowerMetric DMA": {
        "name": "PowerMetric",
        "email": "accountmanagement@powermetric.com.au, rmorse@powermetric.com.au, data.quote@fornrg.com"
    },
    "Origin C&I Electricity": {
        "name": "Origin C&I",
        "email": "MIContracts@originenergy.com.au, data.quote@fornrg.com"
    },
    "Origin SME Electricity": {
        "name": "Origin SME",
        "email": "MIContracts@originenergy.com.au, data.quote@fornrg.com"  # Add if needed
    },
    "BlueNRG SME Electricity": {
        "name": "BlueNRG SME",
        "email": "data.quote@fornrg.com"
    },
    "Origin C&I Gas": {
        "name": "Origin C&I",
        "email": "MIContracts@originenergy.com.au, data.quote@fornrg.com"
    },
    "Momentum C&I Electricity": {
        "name": "Momentum",
        "email": "contracts.administration@momentum.com.au, data.quote@fornrg.com"
    },
    "CovaU SME Gas": {
        "name": "CovaU",
        "email": "corp.sales@covau.com.au, data.quote@fornrg.com"
    },
    "CovaU SME Electricity": {
        "name": "CovaU",
        "email": "corp.sales@covau.com.au, data.quote@fornrg.com"
    },
    "Veolia Waste": {
        "name": "Veolia",
        "email": "ric.luiyf@veolia.com, business@acesolutions.com.au"
    },
    "Alinta C&I Electricity": {
        "name": "Alinta",
        "email": "Andrew.Barnes@alintaenergy.com.au, Moulee.Siriharan@alintaenergy.com.au, data.quote@fornrg.com"
    },
    "Alinta C&I Gas": {
        "name": "Alinta",
        "email": "Andrew.Barnes@alintaenergy.com.au, Moulee.Siriharan@alintaenergy.com.au, data.quote@fornrg.com"
    },
    "Other": {
        "name": "Other",
        "email": "members@acesolutions.com.au, data.quote@fornrg.com, morgan.h@acesolutions.com.au"
    },
}

# EOI email mappings
EOI_EMAIL_MAPPINGS = {
    "Direct Meter Agreement": {
        "name": "DMA Supplier",
        "email": "data.quote@fornrg.com"
    },
    "Cleaning Robot": {
        "name": "Cleaning Tech",
        "email": "cleantech@supplier.com"
    },
    "Inbound Digital Voice Agent": {
        "name": "Voice Tech",
        "email": "voicetech@supplier.com"
    },
    "Cooking Oil Used Oil": {
        "name": "Oil Recycling",
        "email": "oilrecycling@supplier.com"
    },
    "Referral Distribution Program": {
        "name": "Distribution Partner",
        "email": "distribution@partner.com"
    },
    "Solar Energy PPA": {
        "name": "Solar Energy",
        "email": "data.quote@fornrg.com"
    },
    "Self Managed Certificates": {
        "name": "Certificate Management",
        "email": "certificates@supplier.com"
    },
    "Telecommunication": {
        "name": "Telecom",
        "email": "telecom@supplier.com"
    },
    "Wood Pallet": {
        "name": "Wood Pallet",
        "email": "woodpallet@supplier.com"
    },
    "Wood Cut": {
        "name": "Wood Processing",
        "email": "woodprocessing@supplier.com"
    },
    "Baled Cardboard": {
        "name": "Cardboard Recycling",
        "email": "cardboard@recycler.com"
    },
    "Loose Cardboard": {
        "name": "Cardboard Recycling",
        "email": "cardboard@recycler.com"
    },
    "Large Generation Certificates Trading": {
        "name": "LGC Trading",
        "email": "lgc@trading.com"
    },
    "GHG Action Plan": {
        "name": "Environmental",
        "email": "environment@supplier.com"
    },
    "Government Incentives Vic G4": {
        "name": "Government",
        "email": "government@supplier.com"
    },
    "Self Managed VEECs": {
        "name": "VEEC Management",
        "email": "veec@management.com"
    },
    "Demand Response": {
        "name": "Demand Response",
        "email": "demandresponse@supplier.com"
    },
    "Waste Organic Recycling": {
        "name": "Organic Waste",
        "email": "organicwaste@recycler.com"
    },
    "Waste Grease Trap": {
        "name": "Grease Trap",
        "email": "greasetrap@supplier.com"
    },
    "Used Wax Cardboard": {
        "name": "Wax Cardboard",
        "email": "waxcardboard@recycler.com"
    },
    "Vic CDS Scheme": {
        "name": "CDS Scheme",
        "email": "cds@scheme.com"
    },
    "New Placeholder Template": {
        "name": "Template",
        "email": "members@acesolutions.com.au, data.quote@fornrg.com"
    }
}

# Default fallback email
DEFAULT_EMAIL = {
    "name": "Unknown Supplier",
    "email": "members@acesolutions.com.au"
}

def find_supplier_email_for_agreement(contract_type: str, agreement_type: str) -> tuple[str, str, bool]:
    """
    Find the supplier email address based on contract type and agreement type
    
    Args:
        contract_type: Type of contract/EOI
        agreement_type: Type of agreement - either "contract" or "eoi"
        
    Returns:
        Tuple of (email_address, resolved_name, is_default)
    """
    logger.info(f"Looking up email for contract type: '{contract_type}', agreement type: '{agreement_type}'")
    
    # Select the appropriate mapping based on agreement type
    if agreement_type == "eoi":
        mapping = EOI_EMAIL_MAPPINGS
    else:
        mapping = CONTRACT_EMAIL_MAPPINGS
    
    # Try exact match first
    if contract_type in mapping:
        supplier_info = mapping[contract_type]
        logger.info(f"Exact match found: {supplier_info['name']}")
        return supplier_info["email"], supplier_info["name"], False
    
    # Try case-insensitive match
    for key, value in mapping.items():
        if key.lower() == contract_type.lower():
            logger.info(f"Case-insensitive match found: {value['name']}")
            return value["email"], value["name"], False
    
    # Use default email if no match found
    logger.warning(f"No match found for contract type: '{contract_type}', using default email")
    return DEFAULT_EMAIL["email"], DEFAULT_EMAIL["name"], True

@tool
def send_supplier_signed_agreement(
    file_path: str,
    business_name: str,
    contract_type: str,
    agreement_type: str = "contract"
) -> str:
    """Send a signed supplier agreement (Contract or EOI) to a supplier via email.

    Args:
        file_path: Path to the signed agreement file to be sent
        business_name: Name of the business (optionally with identifier: "Business Name NMI: 12345" or "Business Name MIRN: 12345")
        contract_type: Type of contract/EOI (e.g., PowerMetric DMA, Direct Meter Agreement)
        agreement_type: Type of agreement - either "contract" or "eoi" (default: "contract")
    """
    logger.info(f"Processing signed agreement: {contract_type} ({agreement_type})")
    
    # Parse business name and identifier if present
    nmi_match = re.search(r'NMI:\s*(\d+)', business_name)
    mirn_match = re.search(r'MIRN:\s*(\d+)', business_name)
    
    if nmi_match:
        identifier = nmi_match.group(1)
        identifier_type = "nmi"
        actual_business_name = business_name[:nmi_match.start()].strip()
    elif mirn_match:
        identifier = mirn_match.group(1)
        identifier_type = "mirn"
        actual_business_name = business_name[:mirn_match.start()].strip()
    else:
        identifier = None
        identifier_type = None
        actual_business_name = business_name.strip()
    
    # Get supplier email using the mapping
    supplier_email, resolved_supplier_name, is_default = find_supplier_email_for_agreement(contract_type, agreement_type)
    logger.info(f"Resolved supplier email: {supplier_email} for {resolved_supplier_name} (default: {is_default})")
    
    # Prepare file and payload
    try:
        files = {
            "file": (
                file_path.split("/")[-1],  # Get filename from path
                open(file_path, "rb"),
                "application/octet-stream",
            )
        }
    except Exception as e:
        logger.error(f"Error opening file: {str(e)}")
        return f"❌ Error: Could not open file at {file_path}"

    payload = {
        "business_name": actual_business_name,
        "contract_type": contract_type,
        "agreement_type": agreement_type,
        "supplier_email": supplier_email,
        "resolved_supplier_name": resolved_supplier_name,
    }
    
    # Add identifier if present
    if identifier and identifier_type:
        payload[identifier_type] = identifier

    # Send the request
    try:
        response = requests.post(
            "https://membersaces.app.n8n.cloud/webhook/email-supplier",
            data=payload,
            files=files,
        )
        
        if response.status_code == 200:
            # Build success message
            document_name = "EOI" if agreement_type == "eoi" else "contract"
            
            # Heading
            success_msg = f"""✅ **The signed {document_name} has been successfully sent:**\n\n"""
            # Document details
            success_msg += f"📄 **Document:** {contract_type}\n"
            success_msg += f"🏢 **Business:** {actual_business_name}\n"
            success_msg += f"✉️ **Sent to:** {supplier_email}\n"
            success_msg += f"🏷️ **Supplier:** {resolved_supplier_name}"
            # Identifier if present
            if identifier and identifier_type:
                success_msg += f"\n🔢 **{identifier_type.upper()}:** {identifier}"
            # Warning if default
            if is_default:
                success_msg += f"\n\n⚠️ **Note:** '{contract_type}' was not recognized. The {document_name} will be sent to our general members email ({supplier_email}) for manual processing."
            # Log the success
            logger.info(f"Successfully sent {document_name} to {resolved_supplier_name}")
            # Parse response if it contains additional info
            try:
                response_data = response.json()
                if "message" in response_data:
                    logger.info(f"API Response: {response_data['message']}")
            except:
                pass
            # --- Drive Filing Integration ---
            contract_type_to_filing_type = {
                "C&I Electricity": "signed_CI_E",
                "SME Electricity": "signed_SME_E", 
                "C&I Gas": "signed_CI_G",
                "SME Gas": "signed_SME_G",
                "Waste": "signed_WASTE",
                "Oil": "signed_OIL",
                "DMA": "signed_DMA",
            }
            filing_type = contract_type_to_filing_type.get(contract_type)
            drive_filing_result = ""
            if filing_type:
                business_info = get_business_information(actual_business_name)
                try:
                    drive_filing_result = drive_filing({
                        "file_path": file_path,
                        "business_name": actual_business_name,
                        "filing_type": filing_type,
                        "business_info": business_info
                    })
                    success_msg += f"\n\n**Drive Filing Result:** {drive_filing_result}"
                except Exception as e:
                    logger.error(f"Drive filing error: {str(e)}")
                    success_msg += f"\n\n**Drive Filing Error:** {str(e)}"
            else:
                success_msg += "\n\n(No drive filing performed: contract type not mapped)"
            # --- End Drive Filing Integration ---
            return success_msg
        else:
            logger.error(f"Failed to send agreement. Status code: {response.status_code}")
            return f"❌ Error: Failed to send signed {agreement_type} to supplier. Status code: {response.status_code}"
            
    except Exception as e:
        logger.error(f"Error sending request: {str(e)}")
        return f"❌ Error: Failed to send signed {agreement_type} - {str(e)}"
    finally:
        # Close the file
        if 'files' in locals() and 'file' in files:
            try:
                files['file'][1].close()
            except:
                pass