from langchain_core.tools import tool
import requests


@tool
def maximum_demand_generation(
    new_maximum_demand_invoice_file_path: str,
    old_maximum_demand_invoice_file_path: str,
    invoice_id: str,
    old_capacity_kVA: str,
    old_maximum_demand_cost: str,
    new_capacity_kVA: str,
    new_maximum_demand_cost: str,
    start_date: str = None,
) -> str:
    """Generate a maximum demand comparison document. You will only call this tool when the user uploads the new and old maximum demand invoices.

    Args:
        invoice_id: The ID of the invoice (e.g., rec1TmTM7OSF0be9n)
        new_maximum_demand_invoice_file_path: The path to the new maximum demand invoice. Mandatory
        old_maximum_demand_invoice_file_path: The path to the old maximum demand invoice. Mandatory
        old_capacity_kVA: The old capacity in kVA as a string (e.g., "198.28")
        old_maximum_demand_cost: The old maximum demand cost as a string (e.g., "15.49")
        new_capacity_kVA: The new capacity in kVA as a string (e.g., "158.30")
        new_maximum_demand_cost: The new maximum demand cost as a string (e.g., "16.00")
        start_date: The start date in DD/MM/YYYY format (e.g., "01/12/2024")

    Returns:
        str: Response message indicating success or failure of sending the report
    """

    payload = {
        "invoice_id": invoice_id,
        "old_capacity_kVA": old_capacity_kVA,
        "old_maximum_demand_cost": old_maximum_demand_cost,
        "new_capacity_kVA": new_capacity_kVA,
        "new_maximum_demand_cost": new_maximum_demand_cost,
        "start_date": start_date,
    }

    files = {
        "new_maximum_demand_invoice_path": (
            "new_maximum_demand_invoice.pdf",
            open(new_maximum_demand_invoice_file_path, "rb"),
            "application/pdf",
        ),
        "old_maximum_demand_invoice_path": (
            "old_maximum_demand_invoice.pdf",
            open(old_maximum_demand_invoice_file_path, "rb"),
            "application/pdf",
        ),
    }

    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/generate-maximum-demand",
        data=payload,
        files=files,
    )

    if response.status_code != 200:
        raise Exception(
            f"Failed to generate maxium demand comparison. Status code: {response.status_code}"
        )

    data = response.json()
    pdf_document_link = data.get("pdf_document_link")
    spreadsheet_document_link = data.get("spreadsheet_document_link")

    return f"The new maximum demand comparison document has been successfully generated. You can access the PDF comparison here: {pdf_document_link} and the spreadsheet here: {spreadsheet_document_link}"
