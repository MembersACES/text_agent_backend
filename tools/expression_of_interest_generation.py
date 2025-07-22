from langchain_core.tools import tool
import requests
from enum import Enum
import datetime


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
    Commercial_Cleaning_Bot = (
        "Commercial Cleaning Bot Template",
        "1_4gXhi8eDv7N7IGKP4iavGnJ8oppqOU2BeZIDgqVCMo",
    )


@tool
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
) -> str:
    """Generates an Expression of Interest for a specific business with detailed information.

    Args:
        business_name
        abn
        trading_as
        postal_address
        site_address
        telephone
        email
        contact_name: Name of the primary contact
        position: Position/role of the contact person
        expression_type: Type of expression of interest. Must be one of: DIRECT_METER_AGREEMENT, CLEANING_ROBOT, INBOUND_DIGITAL_VOICE_AGENT, COOKING_OIL_USED_OIL, REFERRAL_DISTRIBUTION_PROGRAM, SOLAR_ENERGY_PPA, SELF_MANAGED_CERTIFICATES, TELECOMMUNICATION, WOOD_PALLET, WOOD_CUT, BALED_CARDBOARD, LOOSE_CARDBOARD, LARGE_GENERATION_CERTIFICATES, GHG_ACTION_PLAN, GOVERNMENT_INCENTIVES_VIC_G4, SELF_MANAGED_VEECS, DEMAND_RESPONSE, WASTE_ORGANIC_RECYCLING, WASTE_GREASE_TRAP, USED_WAX_CARDBOARD, VIC_CDS_SCHEME, NEW_PLACEHOLDER_TEMPLATE
        client_folder_url: URL of the client folder
    """
    try:
        enum_type = ExpressionOfInterestType[expression_type]
        expression_type_name, expression_type_template_id = (
            enum_type.value
        )
    except KeyError:
        return f"Invalid expression type. Must be one of: {', '.join(ExpressionOfInterestType.__members__.keys())}"

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
        "template_id": expression_type_template_id,
        "file_name": f"{expression_type_name} EOI for {business_name}",
    }

    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/document-generation-2", json=payload
    )

    if response.status_code == 404:
        return "Sorry but couldn't find that business name"

    data = response.json()
    document_link = data.get("document_link")

    return f'The {expression_type_name} EOI for "{business_name}" has been successfully generated. You can access it here: {document_link} and this is the link of the Google Drive Folder: {client_folder_url}. If you need further assistance or details, please let me know!'
