from langchain_core.tools import tool
import requests


@tool
def electricity_ci_comparaison_generation(
    invoice_id: str,
    offer_validity: str,
    offer_retailer: str,
    offer_period_1: str,
    offer_period_2: str,
    offer_period_3: str,
    peak_rate_1: str,
    peak_rate_2: str,
    peak_rate_3: str,
    off_peak_1: str,
    off_peak_2: str,
    off_peak_3: str,
    shoulder_rate_1: str,
    shoulder_rate_2: str,
    shoulder_rate_3: str,
) -> str:
    """Generate an electricity C&I comparison document.

    Args:
        invoice_id: Unique invoice identifier.
        offer_validity: Offer validity period.
        offer_retailer: Retailer providing the offer.
        offer_period_1: First offer period (e.g., peak hours).
        offer_period_2: Second offer period (e.g., off-peak hours).
        offer_period_3: Third offer period (e.g., shoulder hours).
        peak_rate_1: Peak rate for the first period.
        peak_rate_2: Peak rate for the second period.
        peak_rate_3: Peak rate for the third period.
        off_peak_1: Off-peak rate for the first period.
        off_peak_2: Off-peak rate for the second period.
        off_peak_3: Off-peak rate for the third period.
        shoulder_rate_1: Shoulder rate for the first period.
        shoulder_rate_2: Shoulder rate for the second period.
        shoulder_rate_3: Shoulder rate for the third period.

    Rates should be strings representing monetary values (e.g., "0.15" for 15 cents per kWh).
    """
    payload = {
        "invoice_id": invoice_id,
        "offer_validity": offer_validity,
        "offer_retailer": offer_retailer,
        "offer_period_1": offer_period_1,
        "offer_period_2": offer_period_2,
        "offer_period_3": offer_period_3,
        "peak_rate_1": peak_rate_1,
        "peak_rate_2": peak_rate_2,
        "peak_rate_3": peak_rate_3,
        "off_peak_1": off_peak_1,
        "off_peak_2": off_peak_2,
        "off_peak_3": off_peak_3,
        "shoulder_rate_1": shoulder_rate_1,
        "shoulder_rate_2": shoulder_rate_2,
        "shoulder_rate_3": shoulder_rate_3,
    }

    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/generate-electricity-ci-comparaison",
        json=payload,
    )

    if response.status_code != 200:
        raise Exception(
            f"Failed to generate electricity comparison. Status code: {response.status_code}"
        )

    data = response.json()
    pdf_document_link = data.get("pdf_document_link")
    spreadsheet_document_link = data.get("spreadsheet_document_link")

    return f"The Electricity C&I Comparison document has been successfully generated. You can access the PDF here: {pdf_document_link} and the spreadsheet here: {spreadsheet_document_link}"
