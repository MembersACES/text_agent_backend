# tools/document_generation.py

import requests
import datetime
import logging
from enum import Enum
from typing import Dict, Any

logger = logging.getLogger(__name__)

class ExpressionOfInterestType(Enum):
    DIRECT_METER_AGREEMENT = (
        "Direct Meter Agreement",
        "1bw_IP8xg6MfbQPihEgk25hTc5EZ92VVFssopB7sBaGY",
    )
    CLEANING_ROBOT = (
        "Cleaning Robot",
        "1_4gXhi8eDv7N7IGKP4iavGnJ8oppqOU2BeZIDgqVCMo",
    )
    INBOUND_DIGITAL_VOICE_AGENT = (
        "Inbound Digital Voice Agent",
        "1XJlkGMHOZv9Ll7BPyKvZyfuJTV_FqAs04gruafpCdaE"
    )
    COOKING_OIL_USED_OIL = (
        "Cooking Oil Used Oil",
        "1khBP_VHw6buangKFru3f_CZTu_W8wc2Zw2hn59TVNyU",
    )
    REFERRAL_DISTRIBUTION_PROGRAM = (
        "Referral Distribution Program",
        "1P4dCFDyzw5pLetx0idRU4xkwPUO4Ytr8ICnq-6d_8Mw",
    )
    SOLAR_ENERGY_PPA = (
        "Solar Energy PPA",
        "1mF-6JmAnhxAavB8YTRU-deCGK_CEiaETSkaNpguahVc",
    )
    SELF_MANAGED_CERTIFICATES = (
        "Self Managed Certificates",
        "1jtQ4jpQ7I3Us3i1jVpUH2M8wgPWS1lMbRwVOqvqtAZQ",
    )
    TELECOMMUNICATION = (
        "Telecommunication",
        "176ssVxAYm1yyPJ7IGLVv6bdmIPuw4K3fWRHEC4S9vXo",
    )
    WOOD_PALLET = (
        "Wood Pallet",
        "1_1NMaW0CoQaB7q0B2yJqpzf39p5GaZg9HGRr6d4Rwa0"
    )
    WOOD_CUT = (
        "Wood Cut",
        "1FtXqKo1aQAM06RCxiFwAGGvCE1_ZmWqotlDvA_m7Mgo",
    )
    BALED_CARDBOARD = (
        "Baled Cardboard",
        "1CvOlWUC2NiIUuwmtpd-To3ejjvInPADFz84Rp_JOs_M",
    )
    LOOSE_CARDBOARD = (
        "Loose Cardboard",
        "1NawS4WSMvJ0Gj8N7cgXZ5KSZqYF7MMb2vVrH_COK66w",
    )
    LARGE_GENERATION_CERTIFICATES = (
        "Large Generation Certificates Trading",
        "1SJkLFK_w3gYGa-TJuIxX7GOZrJbVhAWupezCjBqgRfQ",
    )
    GHG_ACTION_PLAN = (
        "GHG Action Plan",
        "1OK-xq2DacKhY2eo3p3GyTHZCl6ttlacE9_M93IcUlaU",
    )
    GOVERNMENT_INCENTIVES_VIC_G4 = (
        "Government Incentives Vic G4",
        "1xa_nCGjyjMkNglFCas2Jhqf5fpr-n6ky-_fjX6GKGNw",
    )
    SELF_MANAGED_VEECS = (
        "Self Managed VEECs",
        "1jfS65PUTkyit41LTmSI_0FNnN0Ku-5jgoha3Poj8RyM",
    )
    DEMAND_RESPONSE = (
        "Demand Response",
        "1cquljcsNOrYtsBK9ziIuN56z9Pm9bjg97GRywsrihUk",
    )
    WASTE_ORGANIC_RECYCLING = (
        "Waste Organic Recycling",
        "1D8dur5ISvTlLSgD8ypYNZOnCcnhoBe15kxcbRecLrUg",
    )
    WASTE_GREASE_TRAP = (
        "Waste Grease Trap",
        "14qOj6Vs8x7kgN1uNhzaCUuQp7ACXhYuOYfDnYp2J1CA",
    )
    USED_WAX_CARDBOARD = (
        "Used Wax Cardboard",
        "1mAV5_Efn8nYNAhO3AWy-WFl5qZqzolzP1-ttHeFlMaY",
    )
    VIC_CDS_SCHEME = (
        "Vic CDS Scheme",
        "1YsPjYenhoSbwhCQ5pAI__UxjQv8mgGzUB_5-JCSIQwA",
    )
    COMMERCIAL_CLEANING_BOT = (
        "Commercial Cleaning Bot Template",
        "1_4gXhi8eDv7N7IGKP4iavGnJ8oppqOU2BeZIDgqVCMo",
    )

def generate_document(
    business_name: str,
    abn: str,
    trading_as: str,
    postal_address: str,
    site_address: str,
    telephone: str,
    email: str,
    contact_name: str,
    position: str,
    client_folder_url: str,
    template_id: str,
    document_type: str
) -> Dict[str, Any]:
    """
    Generate a document using the provided template and business information.
    
    Args:
        business_name: Name of the business
        abn: Australian Business Number
        trading_as: Trading name
        postal_address: Postal address
        site_address: Physical site address
        telephone: Phone number
        email: Email address
        contact_name: Primary contact name
        position: Contact person's role
        client_folder_url: Client's Google Drive folder URL
        template_id: Google Docs template ID
        document_type: Type of document being generated
        
    Returns:
        Dict containing success status, message, and document link
    """
    logger.info(f"Generating {document_type} for {business_name}")
    
    current_date = datetime.datetime.now()
    current_month = current_date.strftime("%b")
    current_year = current_date.year

    payload = {
        "data": {
            "business_name": business_name,
            "trading_as": trading_as,
            "abn": abn,
            "postal_address": postal_address,
            "site_address": site_address,
            "telephone": telephone,
            "email": email,
            "contact_name": contact_name,
            "position": position,
            "client_folder_url": client_folder_url,
            "current_month": current_month,
            "current_year": current_year,
        },
        "template_id": template_id,
        "file_name": f"{document_type} for {business_name}",
    }

    try:
        response = requests.post(
            "https://membersaces.app.n8n.cloud/webhook/document-generation-2", 
            json=payload,
            timeout=30
        )
        
        if response.status_code == 404:
            return {
                "status": "error",
                "message": f"Could not find business name: {business_name}",
                "document_link": None
            }
        
        if response.status_code != 200:
            return {
                "status": "error", 
                "message": f"Document generation failed with status {response.status_code}",
                "document_link": None
            }

        data = response.json()
        document_link = data.get("document_link")
        
        if not document_link:
            return {
                "status": "error",
                "message": "Document generated but no link returned",
                "document_link": None
            }

        logger.info(f"Successfully generated {document_type} for {business_name}")
        
        return {
            "status": "success",
            "message": f'The {document_type} for "{business_name}" has been successfully generated.',
            "document_link": document_link,
            "client_folder_url": client_folder_url
        }
        
    except requests.exceptions.Timeout:
        logger.error(f"Timeout generating {document_type} for {business_name}")
        return {
            "status": "error",
            "message": "Document generation timed out. Please try again.",
            "document_link": None
        }
    except Exception as e:
        logger.error(f"Error generating {document_type} for {business_name}: {str(e)}")
        return {
            "status": "error",
            "message": f"Error generating document: {str(e)}",
            "document_link": None
        }

def loa_generation(
    business_name: str,
    abn: str,
    trading_as: str,
    postal_address: str,
    site_address: str,
    telephone: str,
    email: str,
    contact_name: str,
    position: str,
    client_folder_url: str,
) -> Dict[str, Any]:
    """Generate Letter of Authority (LOA) document for a business."""
    
    return generate_document(
        business_name=business_name,
        abn=abn,
        trading_as=trading_as,
        postal_address=postal_address,
        site_address=site_address,
        telephone=telephone,
        email=email,
        contact_name=contact_name,
        position=position,
        client_folder_url=client_folder_url,
        template_id="170ZpEktA9fo1H0TkaJYFh7iyreYLOwAmrfP2e0Rk_lg",
        document_type="Letter of Authority"
    )

def service_fee_agreement_generation(
    business_name: str,
    abn: str,
    trading_as: str,
    postal_address: str,
    site_address: str,
    telephone: str,
    email: str,
    contact_name: str,
    position: str,
    client_folder_url: str,
) -> Dict[str, Any]:
    """Generate Service Fee Agreement document for a business."""
    
    return generate_document(
        business_name=business_name,
        abn=abn,
        trading_as=trading_as,
        postal_address=postal_address,
        site_address=site_address,
        telephone=telephone,
        email=email,
        contact_name=contact_name,
        position=position,
        client_folder_url=client_folder_url,
        template_id="1LpJfOVV6z9QNmnkwgBt2DVzizolj89cHRckcB1H4lK0",
        document_type="Service Fee Agreement"
    )

def expression_of_interest_generation(
    business_name: str,
    abn: str,
    trading_as: str,
    postal_address: str,
    site_address: str,
    telephone: str,
    email: str,
    contact_name: str,
    position: str,
    expression_type: str,
    client_folder_url: str,
) -> Dict[str, Any]:
    """Generate Expression of Interest for a specific business with detailed information."""
    
    try:
        # Convert string to enum key format
        enum_key = expression_type.upper().replace(" ", "_").replace("-", "_")
        enum_type = ExpressionOfInterestType[enum_key]
        expression_type_name, expression_type_template_id = enum_type.value
    except KeyError:
        valid_types = [e.value[0] for e in ExpressionOfInterestType]
        return {
            "status": "error",
            "message": f"Invalid expression type. Must be one of: {', '.join(valid_types)}",
            "document_link": None
        }

    return generate_document(
        business_name=business_name,
        abn=abn,
        trading_as=trading_as,
        postal_address=postal_address,
        site_address=site_address,
        telephone=telephone,
        email=email,
        contact_name=contact_name,
        position=position,
        client_folder_url=client_folder_url,
        template_id=expression_type_template_id,
        document_type=f"{expression_type_name} EOI"
    )

def ghg_offer_generation(
    business_name: str,
    abn: str,
    trading_as: str,
    postal_address: str,
    site_address: str,
    telephone: str,
    email: str,
    contact_name: str,
    position: str,
    client_folder_url: str,
) -> Dict[str, Any]:
    """Generate GHG Offer document for a business."""
    
    # Note: You'll need to provide the actual template ID for GHG offers
    return generate_document(
        business_name=business_name,
        abn=abn,
        trading_as=trading_as,
        postal_address=postal_address,
        site_address=site_address,
        telephone=telephone,
        email=email,
        contact_name=contact_name,
        position=position,
        client_folder_url=client_folder_url,
        template_id="YOUR_GHG_TEMPLATE_ID_HERE",  # Replace with actual template ID
        document_type="GHG Offer"
    )

# Utility function to get available EOI types
def get_available_eoi_types():
    """Get all available Expression of Interest types."""
    return [e.value[0] for e in ExpressionOfInterestType]