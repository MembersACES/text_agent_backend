from langchain_core.tools import tool
import requests
from typing import Optional, Dict, Any


@tool
def waste_comparaison_generation(
    invoice_id: str,
    cost_comparison_estimated_annual_spend_current_cost: str,
    cost_comparison_estimated_annual_spend_new_cost: str,
    current_diversion_annual_landfill: str,
    new_diversion_annual_landfill: str,
    current_diversion_landfill_diversion: str,
    new_diversion_landfill_diversion: str,
    current_total: str,
    new_total: str,
    current_diversion_percentage: str,
    new_diversion_percentage: str,
) -> str:
    """Generate a waste comparison document. You will need to get the latest invoice ID information first and the user needs to confirm the invoice details.
    Then, You will use this tool to generate the waste comparison document.

    Args:
        invoice_id: Invoice ID
        cost_comparison_estimated_annual_spend_current_cost: Cost Comparison Estimated Annual Spend Current Cost
        cost_comparison_estimated_annual_spend_new_cost: Cost Comparison Estimated Annual Spend New Cost
        current_diversion_annual_landfill: Current Diversion Annual Landfill
        new_diversion_annual_landfill: New Diversion Annual Landfill
        current_diversion_landfill_diversion: Current Diversion Landfill Diversion
        new_diversion_landfill_diversion: New Diversion Landfill Diversion
        current_total: Current Total
        new_total: New Total
        current_diversion_percentage: Current Diversion %
        new_diversion_percentage: New Diversion %
    """
    payload = {
        "invoice_id": invoice_id,
        "cost_comparison_estimated_annual_spend_current_cost": cost_comparison_estimated_annual_spend_current_cost,
        "cost_comparison_estimated_annual_spend_new_cost": cost_comparison_estimated_annual_spend_new_cost,
        "current_diversion_annual_landfill": current_diversion_annual_landfill,
        "new_diversion_annual_landfill": new_diversion_annual_landfill,
        "current_diversion_landfill_diversion": current_diversion_landfill_diversion,
        "new_diversion_landfill_diversion": new_diversion_landfill_diversion,
        "current_total": current_total,
        "new_total": new_total,
        "current_diversion_percentage": current_diversion_percentage,
        "new_diversion_percentage": new_diversion_percentage,
    }

    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/generate-waste-comparaison",
        json=payload,
    )

    if response.status_code != 200:
        raise Exception(
            f"Failed to generate waste comparison. Status code: {response.status_code}"
        )

    data = response.json()
    pdf_document_link = data.get("pdf_document_link")
    spreadsheet_document_link = data.get("spreadsheet_document_link")

    return f"The Waste Comparison document has been successfully generated. You can access the PDF here: {pdf_document_link} and the spreadsheet here: {spreadsheet_document_link}"
