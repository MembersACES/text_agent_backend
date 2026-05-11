# tools/document_generation.py

import re
import requests
import datetime
import logging
from enum import Enum
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


# kg CO₂-e per kWh grid offset (solar cleaning testimonial methodology)
SOLAR_GRID_CO2_KG_PER_KWH = 0.75


def _format_solar_pv_kw_headline_fragment(pv_user: str) -> str:
    """
    User enters capacity only (e.g. 99.6); output is always '99.6 kW' for the Doc headline.
    If they already included kW, strip it first so we do not double up.
    """
    t = (pv_user or "").strip()
    if not t:
        return ""
    t = re.sub(r"\s*kw\s*$", "", t, flags=re.IGNORECASE).strip()
    if not t:
        return ""
    return f"{t} kW"


def _build_solar_n8n_fields(
    *,
    pre_daily_kwh: float,
    post_daily_kwh: float,
    content: Dict[str, Any],
) -> Dict[str, str]:
    """
    Derived metrics for solar_panel_cleaning testimonials (passed through to n8n merge/replace).

    yield_kwh = post - pre
    production_increase_percent = round((yield / pre) * 100, 1) — same as emission_decrease_percent here
    grid_usage_reduction_kwh_annual = yield * 365
    co2_daily_kg_avoided = yield * SOLAR_GRID_CO2_KG_PER_KWH
    co2_emission_reduction_kg_annual = grid_annual * SOLAR_GRID_CO2_KG_PER_KWH
    co2_emission_reduction_tonnes_si = co2_kg_annual / 1000
    """
    yield_kwh = post_daily_kwh - pre_daily_kwh
    pct = round((yield_kwh / pre_daily_kwh) * 100, 1)
    annual_grid_kwh = yield_kwh * 365.0
    co2_daily_kg = yield_kwh * SOLAR_GRID_CO2_KG_PER_KWH
    co2_annual_kg = annual_grid_kwh * SOLAR_GRID_CO2_KG_PER_KWH
    co2_tonnes_si = co2_annual_kg / 1000.0

    pct_s = f"{pct:.1f}"
    pre_s = f"{pre_daily_kwh:.2f}"
    post_s = f"{post_daily_kwh:.2f}"
    yield_s = f"{yield_kwh:.2f}"
    grid_s = f"{annual_grid_kwh:,.1f}"
    co2_daily_s = f"{co2_daily_kg:.2f}"
    co2_annual_kg_s = f"{co2_annual_kg:,.2f}"
    co2_t_si_s = f"{co2_tonnes_si:,.2f}"

    dot4 = (content.get("key_outcome_dotpoints_5") or "").strip() or (
        "Lower risk of undetected degradation shortening asset life or warranty exposure."
    )

    dot1 = f"Previously measured generation of {pre_s} kWh new generation {post_s} kWh"
    dot2 = f"{pct_s}% increase in electricity production observed for the day {yield_s} kWh gained."
    dot3 = f"{pct_s}% decrease in greenhouse gas emissions {co2_daily_s} kg CO₂ avoided."

    return {
        "solar_pre_daily_generation_kwh": pre_s,
        "solar_post_daily_generation_kwh": post_s,
        "solar_yield_kwh_daily": yield_s,
        "yield_kwh_daily": yield_s,
        "production_increase": pct_s,
        "production_increase_percent": pct_s,
        "estimated_annual_production_increase_percent": pct_s,
        "emission_decrease_percent": pct_s,
        "grid_usage_reduction_kwh_annual": grid_s,
        "grid_usage_reduction_kwh": grid_s,
        "co2_daily_kg_avoided": co2_daily_s,
        "co2_emission_reduction_kg_annual": co2_annual_kg_s,
        "co2_emission_reduction_tonnes_si": co2_t_si_s,
        "key_outcome_dotpoints_1": dot1,
        "key_outcome_dotpoints_2": dot2,
        "key_outcome_dotpoints_3": dot3,
        "key_outcome_dotpoints_4": dot4,
        "key_outcome_dotpoints_5": "",
    }


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

class EngagementFormType(Enum):
    # Enum member names must match the normalized `engagement_form_type` string:
    # engagement_form_type.upper().replace(" ", "_").replace("-", "_")
    # For "Solar Panel Cleaning" this becomes "SOLAR_PANEL_CLEANING"
    SOLAR_PANEL_CLEANING = (
        "Solar Panel Cleaning",
        "1-udcqTyew1Gavaoa2rF_kRKracMnQ0wbIxEYGNAGvQo",
    )
    TELECOMMUNICATION = (
        "Telecommunication",
        "1vrZFrCUyS_6qlcpsSdJqNabIRrEuqd4VfDOLjbN3QiU",
    )
    VIC_CDS_SCHEME = (
        "VIC CDS Scheme",
        "1dbI51gtl6uUwsdBVtdxVabpfN3olCu5gPA5jvyVfesM",
    )
    GHG = (
        "GHG",
        "1RuIz4FwgsyWorTFrfGLkeq5nHf5HAEFqGf1pPBmQ6mY",
    )
    REFERRAL_DISTRIBUTION_PROGRAM = (
        "Referral Distribution Program",
        "1Brif45hiE650wdy-JWYd2oZ3Elfcwqiw4tapnUeFx0A",
    )
    ERA_ROBOTIC_REFERRAL_PROGRAM = (
        "ERA Robotic Referral Program",
        "1Brif45hiE650wdy-JWYd2oZ3Elfcwqiw4tapnUeFx0A"
    )


def engagement_form_generation(
    business_name: str,
    abn: str,
    trading_as: str,
    postal_address: str,
    site_address: str,
    telephone: str,
    email: str,
    contact_name: str,
    position: str,
    engagement_form_type: str,
    client_folder_url: str,
) -> Dict[str, Any]:
    """Generate Engagement Form for a specific business with detailed information."""
    
    try:
        # Convert string to enum key format
        enum_key = engagement_form_type.upper().replace(" ", "_").replace("-", "_")
        enum_type = EngagementFormType[enum_key]
        engagement_form_type_name, engagement_form_type_template_id = enum_type.value
    except KeyError:
        valid_types = [e.value[0] for e in EngagementFormType]
        return {
            "status": "error",
            "message": f"Invalid engagement form type. Must be one of: {', '.join(valid_types)}",
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
        template_id=engagement_form_type_template_id,
        document_type=f"{engagement_form_type_name} Engagement Form"
    )

# Utility function to get available Engagement Form types
def get_available_engagement_form_types():
    """Get all available Engagement Form types."""
    return [e.value[0] for e in EngagementFormType]


def generate_testimonial_document(
    business_name: str,
    trading_as: str,
    testimonial_business_name: str,
    testimonial_business_name_source: str,
    contact_name: str,
    position: str,
    email: str,
    telephone: str,
    client_folder_url: str,
    solution_type_id: str,
    savings_amount: float,
    abn: str = "",
    postal_address: str = "",
    site_address: str = "",
    pv_system_size: str = "",
    solar_pre_daily_kwh: Optional[float] = None,
    solar_post_daily_kwh: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Generate a testimonial document using the testimonial Google Doc template.
    Merges business info, calculated savings, and solution-type content (from testimonial_solution_content),
    then calls the n8n testimonial-generation webhook.
    """
    from tools.testimonial_solution_content import (
        get_merged_content,
        TESTIMONIAL_TEMPLATE_DOC_ID,
        build_testimonial_file_name,
    )

    current_date = datetime.datetime.now()
    current_year = current_date.year

    content = get_merged_content(solution_type_id)
    if not content:
        return {
            "status": "error",
            "message": (
                f"Unknown solution type: {solution_type_id}. "
                "Redeploy the API service so it includes the latest "
                "`tools/testimonial_solution_content.py` (extra testimonial types are defined there)."
            ),
            "document_link": None,
        }

    monthly_savings = round(float(savings_amount), 2)
    annual_savings = round(monthly_savings * 12, 2)
    net_outcome = annual_savings  # Template may show optional cost later
    display_name = (testimonial_business_name or "").strip() or (business_name or "").strip()
    legal_business_name = (business_name or "").strip()

    type_label = (content.get("solution_type_label") or solution_type_id or "").strip()
    crm_file_name = build_testimonial_file_name(type_label, display_name or "")

    key_outcome_metrics = (content.get("key_outcome_metrics") or "").strip()
    pv_trim = (pv_system_size or "").strip()
    # Subtitle: {key_outcome_metrics} — {business_name}. Solar: "{n} kW PV System Size Clean" (user may enter n or n kW).
    if solution_type_id == "solar_panel_cleaning" and pv_trim:
        kw_fragment = _format_solar_pv_kw_headline_fragment(pv_trim)
        if kw_fragment:
            key_outcome_metrics = f"{kw_fragment} PV System Size Clean".strip()

    solar_extra: Dict[str, str] = {}
    if solution_type_id == "solar_panel_cleaning":
        if not pv_trim or solar_pre_daily_kwh is None or solar_post_daily_kwh is None:
            return {
                "status": "error",
                "message": (
                    "Solar Panel Cleaning requires solar system size (kW number only), "
                    "pre clean daily generation (kWh), and post clean daily generation (kWh)."
                ),
                "document_link": None,
            }
        pre_f = float(solar_pre_daily_kwh)
        post_f = float(solar_post_daily_kwh)
        if pre_f <= 0:
            return {
                "status": "error",
                "message": "Pre clean daily generation must be greater than zero.",
                "document_link": None,
            }
        if post_f <= pre_f:
            return {
                "status": "error",
                "message": "Post clean daily generation must be greater than pre clean daily generation.",
                "document_link": None,
            }
        solar_extra = _build_solar_n8n_fields(
            pre_daily_kwh=pre_f,
            post_daily_kwh=post_f,
            content=dict(content),
        )

    # Build data dict for n8n: keys match template placeholders {{key}}
    monthly_savings_formatted = f"{monthly_savings:,.2f}"
    annual_savings_formatted = f"{annual_savings:,.2f}"
    net_outcome_formatted = f"{net_outcome:,.2f}"

    data = {
        "business_name": display_name or "",
        "legal_business_name": legal_business_name or "",
        "trading_as": trading_as or "",
        "business_name_source": (testimonial_business_name_source or "business_name"),
        "abn": abn or "",
        "postal_address": postal_address or "",
        "site_address": site_address or "",
        "telephone": telephone or "",
        "email": email or "",
        "contact_name": contact_name or "",
        "position": position or "",
        "contact_position": position or "",
        "contact_email": email or "",
        "contact_number": telephone or "",
        "client_folder_url": client_folder_url or "",
        "current_year": str(current_year),
        "solution_type": content.get("solution_type_label", solution_type_id),
        "key_outcome_metrics": key_outcome_metrics,
        "key_challenge_of_solution": content.get("key_challenge_of_solution", ""),
        "key_approach_of_solution": content.get("key_approach_of_solution", ""),
        "key_outcome_of_solution": content.get("key_outcome_of_solution", ""),
        "key_outcome_dotpoints_1": content.get("key_outcome_dotpoints_1", ""),
        "key_outcome_dotpoints_2": content.get("key_outcome_dotpoints_2", ""),
        "key_outcome_dotpoints_3": content.get("key_outcome_dotpoints_3", ""),
        "key_outcome_dotpoints_4": content.get("key_outcome_dotpoints_4", ""),
        "key_outcome_dotpoints_5": content.get("key_outcome_dotpoints_5", ""),
        "conclusion": content.get("conclusion", ""),
        "esg_scope_for_solution": content.get("esg_scope_for_solution", ""),
        "sdg_impact_for_solution": content.get("sdg_impact_for_solution", ""),
        "monthly_savings": monthly_savings_formatted,
        "annual_savings": annual_savings_formatted,
        "net_outcome": net_outcome_formatted,
    }

    if solar_extra:
        data.update(solar_extra)

    payload = {
        "data": data,
        "template_id": TESTIMONIAL_TEMPLATE_DOC_ID,
        "file_name": crm_file_name,
    }

    try:
        response = requests.post(
            "https://membersaces.app.n8n.cloud/webhook/testimonial-generation",
            json=payload,
            timeout=30,
        )
        if response.status_code != 200:
            return {
                "status": "error",
                "message": f"Document generation failed with status {response.status_code}",
                "document_link": None,
            }
        result = response.json()
        document_link = result.get("document_link")
        if not document_link:
            return {
                "status": "error",
                "message": "Document generated but no link returned",
                "document_link": None,
            }
        logger.info(f"Testimonial document generated for {legal_business_name}")
        return {
            "status": "success",
            "message": f'Testimonial for "{display_name}" has been generated.',
            "document_link": document_link,
            "client_folder_url": client_folder_url,
            "testimonial_type": type_label,
            "file_name": crm_file_name,
            "testimonial_business_name": display_name,
            "legal_business_name": legal_business_name,
        }
    except requests.exceptions.Timeout:
        logger.error("Testimonial document generation timed out")
        return {
            "status": "error",
            "message": "Document generation timed out. Please try again.",
            "document_link": None,
        }
    except Exception as e:
        logger.exception("Testimonial document generation failed")
        return {
            "status": "error",
            "message": str(e),
            "document_link": None,
        }