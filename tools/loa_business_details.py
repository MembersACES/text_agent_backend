import logging
import requests
from typing import Dict, Optional
from langchain.tools import tool

logger = logging.getLogger(__name__)

@tool
def get_loa_business_details():
    """Get business details from LOA document processing and return formatted details"""
    try:
        webhook_url = "https://membersaces.app.n8n.cloud/webhook/return_business_details"
        
        response = requests.post(webhook_url)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Raw n8n response data: {data}")
        
        if data and len(data) > 0:
            # Get the first record from the response
            details = data[0] if isinstance(data, list) else data
            
            formatted_response = "**Business Details Retrieved:**\n\n"
            
            # Add business information fields with proper formatting
            if "Business Name" in details:
                formatted_response += f"**Business Name:** {details['Business Name']}\n"
            if "Trading As" in details:
                formatted_response += f"**Trading As:** {details['Trading As']}\n"
            if "Business ABN" in details:
                formatted_response += f"**ABN:** {details['Business ABN']}\n"
            if "Postal Address" in details:
                formatted_response += f"**Postal Address:** {details['Postal Address']}\n"
            if "Site Address" in details:
                formatted_response += f"**Site Address:** {details['Site Address']}\n"
            if "Contact Name" in details:
                formatted_response += f"**Contact Name:** {details['Contact Name']}\n"
            if "Contact Position" in details:
                formatted_response += f"**Position:** {details['Contact Position']}\n"
            if "Contact  Email  :" in details:
                formatted_response += f"**Email:** {details['Contact  Email  :']}\n"
            if "Contact Number:" in details:
                formatted_response += f"**Phone:** {details['Contact Number:']}\n"
            if "Date" in details:
                formatted_response += f"**Date:** {details['Date']}\n"
            
            formatted_response += "\nPlease confirm if these details are correct for the client folder creation."
            
            logger.info(f"Formatted response: {formatted_response}")
            return formatted_response
        else:
            return "No business details found in the response. Please check if the LOA document was processed correctly."
            
    except Exception as e:
        logger.error(f"Error in get_loa_business_details: {str(e)}")
        return f"Error retrieving business details: {str(e)}"