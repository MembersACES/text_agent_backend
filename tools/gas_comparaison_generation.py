from langchain_core.tools import tool
import requests
from typing import Optional, Dict, Any


@tool
def gas_comparaison_generation(
    invoice_id: str,
    offer_1_retailer: str,
    offer_1_validity: str,
    offer_1_period_1: str,
    offer_1_months_1: str,
    offer_1_rate_1: Optional[str] = None,
    offer_1_period_2: Optional[str] = None,
    offer_1_months_2: Optional[str] = None,
    offer_1_rate_2: Optional[str] = None,
    offer_1_period_3: Optional[str] = None,
    offer_1_months_3: Optional[str] = None,
    offer_1_rate_3: Optional[str] = None,
    offer_2_retailer: Optional[str] = None,
    offer_2_validity: Optional[str] = None,
    offer_2_period_1: Optional[str] = None,
    offer_2_months_1: Optional[str] = None,
    offer_2_rate_1: Optional[str] = None,
    offer_2_period_2: Optional[str] = None,
    offer_2_months_2: Optional[str] = None,
    offer_2_rate_2: Optional[str] = None,
    offer_2_period_3: Optional[str] = None,
    offer_2_months_3: Optional[str] = None,
    offer_2_rate_3: Optional[str] = None,
    offer_3_retailer: Optional[str] = None,
    offer_3_validity: Optional[str] = None,
    offer_3_rate_1: Optional[str] = None,
    offer_3_period_1: Optional[str] = None,
    offer_3_months_1: Optional[str] = None,
    offer_3_rate_2: Optional[str] = None,
    offer_3_period_2: Optional[str] = None,
    offer_3_months_2: Optional[str] = None,
    offer_3_rate_3: Optional[str] = None,
    offer_3_period_3: Optional[str] = None,
    offer_3_months_3: Optional[str] = None,
) -> str:
    """Generate a gas comparison document. You will need to get the latest invoice ID information first and the user needs to confirm the invoice details.
    Then, You will use this tool to generate the gas comparison document.

    Args:
        invoice_id: Invoice ID
        offer_[1-3]_retailer: Retailer name (required for offer 1, optional for 2-3)
        offer_[1-3]_validity: Validity period (required for offer 1, optional for 2-3)
        offer_[1-3]_period_[1-3]: Contract periods (period 1 required, 2-3 optional)
        offer_[1-3]_months_[1-3]: Months for each period (required for period 1, optional for 2-3)
        offer_[1-3]_rate_[1-3]: Rate for each period (required for period 1, optional for 2-3)

    Note: All parameters after offer_1_period_1 and its corresponding months and rate are optional.
    Parameters follow pattern offer_[1-3] for up to 3 offers, each with up to 3 periods.
    Each period requires its corresponding months and rate to be specified
    """
    payload = {
        "invoice_id": invoice_id,
        "offer_1_retailer": offer_1_retailer,
        "offer_1_validity": offer_1_validity,
        "offer_1_period_1": offer_1_period_1,
        "offer_1_months_1": offer_1_months_1,
        "offer_1_rate_1": offer_1_rate_1,
    }

    if offer_1_period_2 and offer_1_months_2 and offer_1_rate_2:
        payload.update(
            {
                "offer_1_period_2": offer_1_period_2,
                "offer_1_months_2": offer_1_months_2,
                "offer_1_rate_2": offer_1_rate_2,
            }
        )
    if offer_1_period_3 and offer_1_months_3 and offer_1_rate_3:
        payload.update(
            {
                "offer_1_period_3": offer_1_period_3,
                "offer_1_months_3": offer_1_months_3,
                "offer_1_rate_3": offer_1_rate_3,
            }
        )

    if all(
        [
            offer_2_retailer,
            offer_2_validity,
            offer_2_period_1,
            offer_2_months_1,
            offer_2_rate_1,
        ]
    ):
        payload.update(
            {
                "offer_2_retailer": offer_2_retailer,
                "offer_2_validity": offer_2_validity,
                "offer_2_period_1": offer_2_period_1,
                "offer_2_months_1": offer_2_months_1,
                "offer_2_rate_1": offer_2_rate_1,
            }
        )
        if offer_2_period_2 and offer_2_months_2 and offer_2_rate_2:
            payload.update(
                {
                    "offer_2_period_2": offer_2_period_2,
                    "offer_2_months_2": offer_2_months_2,
                    "offer_2_rate_2": offer_2_rate_2,
                }
            )
        if offer_2_period_3 and offer_2_months_3 and offer_2_rate_3:
            payload.update(
                {
                    "offer_2_period_3": offer_2_period_3,
                    "offer_2_months_3": offer_2_months_3,
                    "offer_2_rate_3": offer_2_rate_3,
                }
            )

    if all(
        [
            offer_3_retailer,
            offer_3_validity,
            offer_3_period_1,
            offer_3_months_1,
            offer_3_rate_1,
        ]
    ):
        payload.update(
            {
                "offer_3_retailer": offer_3_retailer,
                "offer_3_validity": offer_3_validity,
                "offer_3_period_1": offer_3_period_1,
                "offer_3_months_1": offer_3_months_1,
                "offer_3_rate_1": offer_3_rate_1,
            }
        )
        if offer_3_period_2 and offer_3_months_2 and offer_3_rate_2:
            payload.update(
                {
                    "offer_3_period_2": offer_3_period_2,
                    "offer_3_months_2": offer_3_months_2,
                    "offer_3_rate_2": offer_3_rate_2,
                }
            )
        if offer_3_period_3 and offer_3_months_3 and offer_3_rate_3:
            payload.update(
                {
                    "offer_3_period_3": offer_3_period_3,
                    "offer_3_months_3": offer_3_months_3,
                    "offer_3_rate_3": offer_3_rate_3,
                }
            )

    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/generate-gas-comparaison",
        json=payload,
    )

    if response.status_code != 200:
        raise Exception(
            f"Failed to generate gas comparison. Status code: {response.status_code}"
        )

    data = response.json()
    pdf_document_link = data.get("pdf_document_link")
    spreadsheet_document_link = data.get("spreadsheet_document_link")

    return f"The Gas Comparison document has been successfully generated. You can access the PDF here: {pdf_document_link} and the spreadsheet here: {spreadsheet_document_link}"
