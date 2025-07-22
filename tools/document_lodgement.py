import logging
import requests
from typing import Dict, Optional
from langchain.tools import tool

logger = logging.getLogger(__name__)

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
    "WATER": "water_invoice"
}

API_ENDPOINTS = {
    "LOA": "https://aces-api-63gwbzzcdq-km.a.run.app/v1/loa/process-document",
    "PROFIT_SHARING": "https://aces-api-63gwbzzcdq-km.a.run.app/v1/psa/process-document",
    "WASTE": "https://aces-invoice-api-672026052958.australia-southeast2.run.app/v1/waste/process-invoice",
    "COOKING_OIL": "https://aces-invoice-api-672026052958.australia-southeast2.run.app/v1/oil/process-invoice",
    "ELECTRICITY_CI": "https://aces-api-63gwbzzcdq-km.a.run.app/v1/electricity-ci/process-invoice",
    "ELECTRICITY_SME": "https://aces-api-63gwbzzcdq-km.a.run.app/v1/electricity-sme/process-invoice",
    "GAS_CI": "https://aces-api-63gwbzzcdq-km.a.run.app/v1/gas-ci/process-invoice",
    "GAS_SME": "https://aces-api-63gwbzzcdq-km.a.run.app/v1/gas-sme/process-invoice",
    "GREASE_TRAP": "https://aces-api-63gwbzzcdq-km.a.run.app/v1/grease-trap/process-invoice",
    "WATER": "https://aces-api-63gwbzzcdq-km.a.run.app/v1/water/process-invoice"
}

@tool 
def lodge_document(
    file_path: str,
    utility_type: str,
    metadata: Optional[Dict] = None
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
    
    try:
        # Prepare the file for upload
        files = {
            "file": (
                file_path.split("/")[-1],
                open(file_path, "rb"),
                "application/pdf" if file_path.endswith(".pdf") else "application/octet-stream"
            )
        }
        
        # Prepare the payload with any additional metadata
        payload = {}
        if metadata:
            payload.update(metadata)
        
        # Send the document to the appropriate endpoint
        endpoint = API_ENDPOINTS[utility_type]
        response = requests.post(
            endpoint,
            data=payload,
            files=files
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to lodge document. Status code: {response.status_code}")
        
        # Parse the response
        data = response.json()
        
        # Create response message
        filename = file_path.split("/")[-1]
        response_message = f"Document successfully lodged!\n\n"
        response_message += f"File: {filename}\n"
        response_message += f"Utility Type: {utility_type}\n"
        
        # Add any document ID or reference from the response
        if "document_id" in data:
            response_message += f"Document ID: {data['document_id']}\n"
        if "reference" in data:
            response_message += f"Reference: {data['reference']}\n"
        if "confirmation_link" in data:
            response_message += f"View confirmation: {data['confirmation_link']}\n"
        
        return response_message
    
    except Exception as e:
        logger.error(f"Error lodging document: {str(e)}")
        return f"Error: Failed to lodge document - {str(e)}" 
