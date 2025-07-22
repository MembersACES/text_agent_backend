from langchain_core.tools import tool
import requests


@tool
def sppa_comparison_generation(
    invoice_id: str,
    sppa_system_size: str,
    ppa_price: str,
    percentage_reduction_from_grid: str,
) -> str:
    """Generate a solar power purchase agreement comparison document.

    Args:
        invoice_id: Unique invoice identifier.
        sppa_system_size: Solar power purchase agreement system size in kW.
        ppa_price: Power purchase agreement price in c/kWh.
        percentage_reduction_from_grid: Percentage reduction from grid in %.
    """
    payload = {
        "invoice_id": invoice_id,
        "sppa_system_size": sppa_system_size,
        "ppa_price": ppa_price,
        "percentage_reduction_from_grid": percentage_reduction_from_grid,
    }

    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/generate-sppa-comparison",
        json=payload,
    )

    if response.status_code != 200:
        raise Exception(
            f"Failed to generate sppa comparison. Status code: {response.status_code}"
        )

    data = response.json()
    pdf_document_link = data.get("pdf_document_link")
    spreadsheet_document_link = data.get("spreadsheet_document_link")

    return f"The SPPA document has been successfully generated. You can access the PDF comparison here: {pdf_document_link} and the spreadsheet here: {spreadsheet_document_link}"
