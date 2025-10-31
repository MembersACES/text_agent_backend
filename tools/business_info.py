import requests
import logging

def get_file_ids(business_name: str) -> dict:
    webhook_url = "https://membersaces.app.n8n.cloud/webhook/return_fileIDs"
    payload = {"business_name": business_name}
    try:
        response = requests.post(webhook_url, json=payload)
        if response.status_code == 200:
            file_ids_data = response.json()
            if isinstance(file_ids_data, list) and file_ids_data:
                return file_ids_data[0]
            elif isinstance(file_ids_data, dict):
                return file_ids_data
        return {}
    except Exception:
        return {}

def get_business_information(business_name: str) -> dict:
    """
    Get the business information including:
    - record_ID
    - Business Details: Name, Trading Name, ABN
    - Contact Information: Postal Address, Site Address, Telephone, Email
    - Representative Details: Contact Name, Position, LOA Sign Date
    - Business Documents: List of available documents
    - Linked Utilities and their retailers
    - Google Drive folder URL

    Args:
        business_name (str): The name of the business to search for

    Returns:
        Dict containing business information including record_ID and formatted_output
    """
    logger = logging.getLogger(__name__)

    processed_file_ids = {}
    # Send API request
    payload = {"business_name": business_name}
    logger.info(f"Making API call to n8n with payload: {payload}")
    
    try:
        response = requests.post(
            "https://membersaces.app.n8n.cloud/webhook/search-business-info", json=payload
        )
        logger.info(f"API response status code: {response.status_code}")
        logger.info(f"API response content: {response.text}")

        if response.status_code == 404:
            return {"_formatted_output": "Sorry but couldn't find that business name"}

        if not response.text:
            logger.error("Empty response received from API")
            return {"_formatted_output": "Error: Received empty response from the server"}

        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {str(e)}")
            logger.error(f"Raw response content: {response.text}")
            return {"_formatted_output": "Error: Invalid response format from the server"}

        logger.info(f"Parsed API response data: {data}")

        # Get the official business name to use for file ID lookup
        official_business_name = data.get('business_details', {}).get('name', business_name)
        
        # Call the new tool to get file IDs
        file_ids_data = get_file_ids(official_business_name)

        # The webhook returns a list with one dictionary, so we extract it.
        file_ids_dict = {}
        if isinstance(file_ids_data, list) and file_ids_data:
            file_ids_dict = file_ids_data[0]
        elif isinstance(file_ids_data, dict):
            file_ids_dict = file_ids_data

        # Process file IDs for easy access by other functions
        # Map N8N keys to expected keys
        file_mapping = {
            'LOA File ID': 'business_LOA',
            'WIP': 'business_WIP',
            'Floor Plan': 'business_Floor Plan (Exit Map)',
            'Site Profiling': 'business_Site Profling',
            'Service Fee Agreement': 'business_Service Fee Agreement',
            'Initial Strategy': 'business_Initial Strategy',
            'Cleaning Invoice': 'invoice_Cleaning',
            'Telecommunication:': 'invoice_Telecommunication',
            'Oil Invoice': 'invoice_Oil',
            'Water Invoice': 'invoice_Water',
            'SC C&I E': 'contract_C&I Electricity',
            'SC SME E': 'contract_SME Electricity',
            'SC C&I G': 'contract_C&I Gas',
            'SC SME G': 'contract_SME Gas',
            'SC Waste': 'contract_Waste',
            'SC Oil': 'contract_Oil',
            'SC DMA': 'contract_DMA',
            'Amortisation Excel': 'business_amortisation_excel',
            'Amortisation PDF': 'business_amortisation_pdf',
        }
        
        # Process all file IDs from N8N data
        for n8n_key, mapped_key in file_mapping.items():
            file_id = file_ids_dict.get(n8n_key)
            if file_id and file_id.strip():  # Check if file_id exists and is not empty
                file_link = f"https://drive.google.com/file/d/{file_id}/view"
                processed_file_ids[mapped_key] = file_link

        # Prepare LOA file link for Representative Details
        loa_file_id = file_ids_dict.get('LOA File ID')
        loa_file_link = None
        if loa_file_id and loa_file_id.strip():
            loa_file_link = f"https://drive.google.com/file/d/{loa_file_id}/view"

        # Format the response message in a clear and organized way
        formatted_response = f"""Here is the information for {business_name}:

### Business Details:
- **Business Name:** {data.get('business_details', {}).get('name', 'N/A')}
- **Trading As:** {data.get('business_details', {}).get('trading_name', 'N/A')}
- **ABN:** {data.get('business_details', {}).get('abn', 'N/A')}

### Contact Information:
- **Postal Address:** {data.get('contact_information', {}).get('postal_address', 'N/A')}
- **Site Address:** {data.get('contact_information', {}).get('site_address', 'N/A')}
- **Contact Number:** {data.get('contact_information', {}).get('telephone', 'N/A')}
- **Contact Email:** {data.get('contact_information', {}).get('email', 'N/A')}

### Representative Details:
- **Contact Name:** {data.get('representative_details', {}).get('contact_name', 'N/A')}
- **Position:** {data.get('representative_details', {}).get('position', 'N/A')}
- **LOA Sign Date:** {data.get('representative_details', {}).get('signed_date', 'N/A')}
"""
        # Add LOA file link if available (constructed from ID only)
        if loa_file_link:
            formatted_response += f"- **LOA:** [In File]({loa_file_link})\n"
        else:
            formatted_response += f"- **LOA:** Not Available\n"

        formatted_response += "\n### Business Documents:\n"
        
        # Create business documents dict from available files
        business_documents = {}
        
        # Check each document type based on N8N data
        doc_checks = [
            ('Initial Strategy', 'Initial Strategy'),
            ('Site Profling', 'Site Profiling'),  # Note the typo handling
            ('Service Fee Agreement', 'Service Fee Agreement'),
            ('Floor Plan (Exit Map)', 'Floor Plan'),
        ]
        
        for doc_name, n8n_key in doc_checks:
            file_id = file_ids_dict.get(n8n_key)
            business_documents[doc_name] = bool(file_id and file_id.strip())
        
        # Always show Initial Strategy as available (as per original logic)
        if 'Initial Strategy' not in business_documents:
            business_documents['Initial Strategy'] = True
            
        if business_documents:
            for doc_name, status in business_documents.items():
                if status:
                    # Map document name to file ID key
                    doc_key = f"business_{doc_name}"
                    file_link = processed_file_ids.get(doc_key)

                    if file_link:
                        formatted_response += f"- **{doc_name}:** [In File]({file_link})\n"
                    else:
                        formatted_response += f"- **{doc_name}:** In File (Link not found)\n"
                else:
                    formatted_response += f"- **{doc_name}:** Not Available\n"
        else:
            formatted_response += "- No business documents available\n"

        # Add Signed Contracts section
        sc_fields = [
            ("SC C&I E", "C&I Electricity"),
            ("SC SME E", "SME Electricity"),
            ("SC C&I G", "C&I Gas"),
            ("SC SME G", "SME Gas"),
            ("SC Waste", "Waste"),
            ("SC Oil", "Oil"),
            ("SC DMA", "DMA"),
        ]
        formatted_response += "\n### Signed Contracts:\n"
        for sc_key, sc_label in sc_fields:
            sc_file_id = file_ids_dict.get(sc_key)
            if sc_file_id and sc_file_id.strip():
                sc_link = f"https://drive.google.com/file/d/{sc_file_id}/view"
                formatted_response += f"- **{sc_label}:** [In File]({sc_link})\n"
            else:
                formatted_response += f"- **{sc_label}:** Not Available\n"

        wip_file_id = file_ids_dict.get('WIP')
        if wip_file_id and wip_file_id.strip():
            wip_file_link = f"https://drive.google.com/file/d/{wip_file_id}/view"
            processed_file_ids["business_WIP"] = wip_file_link
        
        formatted_response += "\n### Linked Utilities and Retailers:"

        # Add linked utilities and their details
        linked_utilities = data.get('Linked_Details', {}).get('linked_utilities', {})
        utility_retailers = data.get('Linked_Details', {}).get('utility_retailers', {})

        if "Robot Number" in linked_utilities:
            linked_utilities["Robot"] = linked_utilities["Robot Number"]
        
        # Handle all utility types (keeping the original logic)
        utility_types = [
            ('C&I Electricity', 'NMI'),
            ('SME Electricity', 'NMI'),
            ('C&I Gas', 'MRIN'),
            ('SME Gas', 'MRIN'),
            ('Small Gas', 'MRIN'),
            ('Waste', 'Account Number'),
            ('Oil', 'Account Name'),
        ]

        for utility_type, identifier_type in utility_types:
            if utility_type in linked_utilities:
                formatted_response += f"\n\n**{utility_type}:**"
                details = linked_utilities[utility_type]
                if isinstance(details, str):
                    formatted_response += f"\n- {identifier_type}: {details}"
                elif isinstance(details, bool) and details:
                    formatted_response += "\n- Status: In File"
                if utility_type in utility_retailers:
                    retailers = utility_retailers[utility_type]
                    if isinstance(retailers, list):
                        formatted_response += f"\n- Retailer: {', '.join(retailers)}"
                    else:
                        formatted_response += f"\n- Retailer: {retailers}"

        # Handle Robots section
        robot_number = linked_utilities.get('Robot Number')
        robot_supplier = utility_retailers.get('Robot Supplier')
        if robot_number or robot_supplier:
            formatted_response += "\n\n**Robots:**"
            if robot_number:
                if isinstance(robot_number, str) and ',' in robot_number:
                    robot_numbers = [r.strip() for r in robot_number.split(',')]
                    formatted_response += f"\n- Robot Number: {', '.join(robot_numbers)}"
                elif isinstance(robot_number, list):
                    formatted_response += f"\n- Robot Number: {', '.join(robot_number)}"
                else:
                    formatted_response += f"\n- Robot Number: {robot_number}"
            if robot_supplier:
                formatted_response += f"\n- Supplier: {robot_supplier}"

        # Handle Cleaning
        if 'Cleaning' in linked_utilities:
            formatted_response += "\n\n**Cleaning:**"
            details = linked_utilities['Cleaning']
            if isinstance(details, bool):
                formatted_response += f"\n- Status: {'In File' if details else 'Not Available'}"
        else:
            formatted_response += "\n\n**Cleaning:**\n- Status: Not Available"

        # Handle Telecommunication
        if 'Telecommunication' in linked_utilities:
            formatted_response += "\n\n**Telecommunication:**"
            details = linked_utilities['Telecommunication']
            if isinstance(details, bool):
                formatted_response += f"\n- Status: {'In File' if details else 'Not Available'}"
        else:
            formatted_response += "\n\n**Telecommunication:**\n- Status: Not Available"

        formatted_response += f"""

### Google Drive:
- **Folder URL:** {data.get('gdrive', {}).get('folder_url', 'N/A')}

### Information Retrieval
"""

        logger.info(f"Formatted response: {formatted_response}")
        logger.info(f"Processed file IDs: {processed_file_ids}")
        
        # Return both the raw data and formatted output, plus processed file IDs
        return {
            **data,  # Include all raw data
            "_formatted_output": formatted_response,  # Add formatted output
            "_processed_file_ids": processed_file_ids,  # Add processed file IDs for other functions
            "business_documents": business_documents  # Add the business_documents dict
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error in get_business_information: {str(e)}", exc_info=True)
        return {"_formatted_output": "Error: Unable to connect to the server. Please try again later."}
    except Exception as e:
        logger.error(f"Unexpected error in get_business_information: {str(e)}", exc_info=True)
        return {"_formatted_output": "Error: An unexpected error occurred. Please try again later."}