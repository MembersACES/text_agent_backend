from langchain_core.tools import tool
import requests


@tool
def ghg_offer_generation(
    record_id: str,
) -> str:
    """Generate a GHG offer document.

    Args:
        record_id: business record id. Use get_business_information to get the business information
    """
    payload = {
        "record_id": record_id,
    }

    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/generate-ghg-offer",
        json=payload,
    )

    if response.status_code != 200:
        raise Exception(
            f"Failed to generate ghg offer. Status code: {response.status_code}"
        )

    data = response.json()
    pdf_document_link = data.get("pdf_document_link")
    spreadsheet_document_link = data.get("spreadsheet_document_link")

    return f"The GHG Offer document has been successfully generated. You can access the PDF here: {pdf_document_link} and the spreadsheet here: {spreadsheet_document_link}"
