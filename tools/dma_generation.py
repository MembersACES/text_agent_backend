from langchain_core.tools import tool
import requests


@tool
def dma_generation(
    invoice_id: str,
    dma_price: str,
    vas_price: str,
    dma_start_date: str,
    dma_end_date: str,
    dma_term_years: str,
) -> str:
    """Generate a direct meter agreement document and comparison document.

    Args:
        invoice_id: Unique invoice identifier.
        dma_price: Direct meter agreement price.
        vas_price: Value added service price.
        dma_start_date: Direct meter agreement start date.
        dma_end_date: Direct meter agreement end date.
        dma_term_years: Direct meter agreement term in years.
    """
    payload = {
        "invoice_id": invoice_id,
        "dma_price": dma_price,
        "vas_price": vas_price,
        "dma_start_date": dma_start_date,
        "dma_end_date": dma_end_date,
        "dma_term_years": dma_term_years,
    }

    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/generate-dma-comparaison",
        json=payload,
    )

    if response.status_code != 200:
        raise Exception(
            f"Failed to generate dma generation. Status code: {response.status_code}"
        )

    data = response.json()
    pdf_document_link = data.get("pdf_document_link")
    spreadsheet_document_link = data.get("spreadsheet_document_link")
    pdf_DMA_link = data.get("pdf_DMA_link")

    return f"The DMA document has been successfully generated. You can access the PDF comparison here: {pdf_document_link}, the DMA PDF here: {pdf_DMA_link} and the spreadsheet here: {spreadsheet_document_link}"
