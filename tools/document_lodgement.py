import logging
import requests
from typing import Dict, List, Optional
from langchain.tools import tool

logger = logging.getLogger(__name__)

ACES_INVOICE_API_BASE = (
    "https://aces-invoice-api-672026052958.australia-southeast2.run.app"
)
ACES_LEGACY_API_BASE = "https://aces-api-63gwbzzcdq-km.a.run.app"


def _invoice_path(segment: str) -> str:
    return f"{ACES_INVOICE_API_BASE}/v1/{segment}"


# Utility types and their corresponding API endpoints
UTILITY_TYPES = {
    "LOA": "letter_of_authority",
    "PROFIT_SHARING": "profit_sharing_agreement",
    "WASTE": "waste_invoice",
    "GREASE_TRAP": "grease_trap_invoice",
    "COOKING_OIL": "cooking_oil_invoice",
    "ELECTRICITY_CI": "electricity_invoice_ci",
    "ELECTRICITY_SME": "electricity_invoice_sme",
    "GAS_CI": "gas_invoice_ci",
    "GAS_SME": "gas_invoice_sme",
    "WATER": "water_invoice",
}

API_ENDPOINTS = {
    "LOA": _invoice_path("loa/process-document"),
    "PROFIT_SHARING": _invoice_path("psa/process-document"),
    "WASTE": _invoice_path("waste/process-invoice"),
    "COOKING_OIL": _invoice_path("oil/process-invoice"),
    "ELECTRICITY_CI": _invoice_path("electricity-ci-invoice/process-invoice"),
    "ELECTRICITY_SME": _invoice_path("electricity-sme/process-invoice"),
    "GAS_CI": _invoice_path("gas-ci-invoice/process-invoice"),
    "GAS_SME": _invoice_path("gas-sme/process-invoice"),
    "GREASE_TRAP": _invoice_path("grease-trap/process-invoice"),
    "WATER": _invoice_path("water/process-invoice"),
}

LOA_ENDPOINT_FALLBACKS: List[str] = [
    API_ENDPOINTS["LOA"],
    f"{ACES_LEGACY_API_BASE}/v1/loa/process-document",
]


def _endpoints_for_utility(utility_type: str) -> List[str]:
    if utility_type == "LOA":
        return LOA_ENDPOINT_FALLBACKS
    return [API_ENDPOINTS[utility_type]]


@tool
def lodge_document(
    file_path: str,
    utility_type: str,
    metadata: Optional[Dict] = None,
) -> str:
    """
    Lodge a document to the appropriate API endpoint based on utility type.

    Args:
        file_path: The path to the document file to be lodged
        utility_type: Type of utility document (must match UTILITY_TYPES)
        metadata: Optional additional metadata for the document

    Returns:
        str: Confirmation message with any relevant IDs or links
    """
    if utility_type not in UTILITY_TYPES:
        valid_types = ", ".join(UTILITY_TYPES.keys())
        return f"Error: Invalid utility type. Valid types are: {valid_types}"

    if utility_type not in API_ENDPOINTS:
        return f"Error: No API endpoint configured for utility type: {utility_type}"

    endpoints = _endpoints_for_utility(utility_type)
    last_error: Optional[str] = None

    try:
        payload = {}
        if metadata:
            payload.update(metadata)

        content_type = (
            "application/pdf"
            if file_path.endswith(".pdf")
            else "application/octet-stream"
        )
        filename = file_path.split("/")[-1]

        for endpoint in endpoints:
            try:
                with open(file_path, "rb") as file_handle:
                    files = {
                        "file": (filename, file_handle, content_type),
                    }
                    response = requests.post(
                        endpoint,
                        data=payload,
                        files=files,
                    )
                if response.status_code == 200:
                    data = response.json()
                    response_message = "Document successfully lodged!\n\n"
                    response_message += f"File: {filename}\n"
                    response_message += f"Utility Type: {utility_type}\n"
                    if utility_type == "LOA" and endpoint != endpoints[0]:
                        response_message += "(via legacy API fallback)\n"

                    if "document_id" in data:
                        response_message += f"Document ID: {data['document_id']}\n"
                    if "reference" in data:
                        response_message += f"Reference: {data['reference']}\n"
                    if "confirmation_link" in data:
                        response_message += (
                            f"View confirmation: {data['confirmation_link']}\n"
                        )

                    return response_message

                last_error = (
                    f"Failed to lodge document. Status code: {response.status_code}"
                )
                logger.warning(
                    "Lodge document failed at %s: %s", endpoint, last_error
                )
            except Exception as e:
                last_error = str(e)
                logger.warning("Lodge document error at %s: %s", endpoint, e)

        raise Exception(last_error or "Failed to lodge document")

    except Exception as e:
        logger.error(f"Error lodging document: {str(e)}")
        return f"Error: Failed to lodge document - {str(e)}"
