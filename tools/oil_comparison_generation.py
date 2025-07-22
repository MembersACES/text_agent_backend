from langchain_core.tools import tool
import requests
from typing import Optional
from tools.get_business_information import get_business_information

@tool
def oil_comparaison_generation(
    business_name: str,  # Changed from record_id to business_name
    current_usage_oil: str,
    current_cost_oil: str,
    current_monthly_invoice_cost: str,
    new_offer_cost_oil: str,
    current_rebate_usage: Optional[str] = "0",
    current_rebate_rate: Optional[str] = "0",
    new_offer_rebate_rate: Optional[str] = "0",
) -> str:
    """Generate an oil comparison document. You will need to get the latest invoice ID information first and the user needs to confirm the invoice details.
    Then, You will use this tool to generate the oil comparison document.

    Args:
        business_name: Name of the business
        current_usage_oil: Current Usage Oil
        current_cost_oil: Current Cost Oil
        current_monthly_invoice_cost: Total current monthly invoice cost
        new_offer_cost_oil: New offered cost of oil
        current_rebate_usage: Current rebate usage amount (defaults to "0")
        current_rebate_rate: Current rebate rate (defaults to "0")
        new_offer_rebate_rate: New offered rebate rate (defaults to "0")
    """
    
    # First, get the business information to extract record_id
    business_info = get_business_information(business_name)
    
    # Check if we got an error string or actual data
    if isinstance(business_info, str):
        return f"Error getting business information: {business_info}"
    
    # Extract the record_ID from the response
    record_id = business_info.get('record_ID')
    
    if not record_id:
        raise Exception(f"Could not find record ID for business: {business_name}")
    
    # Now generate the comparison using the correct record ID
    payload = {
        "record_id": record_id,
        "current_usage_oil": current_usage_oil,
        "current_cost_oil": current_cost_oil,
        "current_monthly_invoice_cost": current_monthly_invoice_cost,
        "current_rebate_usage": current_rebate_usage,
        "current_rebate_rate": current_rebate_rate,
        "new_offer_cost_oil": new_offer_cost_oil,
        "new_offer_rebate_rate": new_offer_rebate_rate,
    }

    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/generate-oil-comparison",
        json=payload,
    )

    if response.status_code != 200:
        raise Exception(
            f"Failed to generate oil comparison. Status code: {response.status_code}"
        )

    data = response.json()
    pdf_document_link = data.get("pdf_document_link")
    pdf_document_id = data.get("pdf_document_id")
    spreadsheet_document_link = data.get("spreadsheet_document_link")

    return f"The Oil Comparison document has been successfully generated. You can access the PDF here: {pdf_document_link} and the spreadsheet here: {spreadsheet_document_link}. The oil comparison document id is {pdf_document_id}"