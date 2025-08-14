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

        # Prepare LOA file link for Representative Details (use only the ID, not the direct link)
        loa_file_id = file_ids_dict.get('LOA File ID')
        loa_file_link = None
        if loa_file_id:
            loa_file_link = f"https://drive.google.com/file/d/{loa_file_id}/view"
            processed_file_ids["business_LOA"] = loa_file_link

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
        # Add business documents information
        business_documents = data.get('business_documents', {})
        # Check if Initial Strategy file exists and add it to business_documents if missing
        initial_strategy_id = file_ids_dict.get('Initial Strategy')
        if initial_strategy_id and 'Initial Strategy' not in business_documents:
            business_documents['Initial Strategy'] = True
            
        if business_documents:
            for doc_name, status in business_documents.items():
                if status:
                    # Correct potential typos and map to the right key in file_ids_data
                    file_id_key = doc_name.replace(" (Exit Map)", "")
                    if file_id_key == "Site Profling":
                        file_id_key = "Site Profiling"
                    
                    # First, try to get a direct link
                    file_link = file_ids_dict.get(f"{file_id_key} File ID Link")

                    # If no direct link, try to get an ID and construct the link
                    if not file_link:
                        file_id = file_ids_dict.get(file_id_key)
                        if file_id:
                            file_link = f"https://drive.google.com/file/d/{file_id}/view"

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
            if sc_file_id:
                sc_link = f"https://drive.google.com/file/d/{sc_file_id}/view"
                formatted_response += f"- **{sc_label}:** [In File]({sc_link})\n"
            else:
                formatted_response += f"- **{sc_label}:** Not Available\n"

        formatted_response += "\n### Linked Utilities and Retailers:"

        # Add linked utilities and their details
        linked_utilities = data.get('Linked_Details', {}).get('linked_utilities', {})
        utility_retailers = data.get('Linked_Details', {}).get('utility_retailers', {})

        if "Robot Number" in linked_utilities:
            linked_utilities["Robot"] = linked_utilities["Robot Number"]
        
        # Handle C&I Electricity
        if 'C&I Electricity' in linked_utilities:
            formatted_response += "\n\n**C&I Electricity:**"
            details = linked_utilities['C&I Electricity']
            if isinstance(details, str):
                formatted_response += f"\n- NMI: {details}"
            elif isinstance(details, bool) and details:
                formatted_response += "\n- Status: In File"
            if 'C&I Electricity' in utility_retailers:
                retailers = utility_retailers['C&I Electricity']
                if isinstance(retailers, list):
                    formatted_response += f"\n- Retailer: {', '.join(retailers)}"
                else:
                    formatted_response += f"\n- Retailer: {retailers}"

        # Handle SME Electricity
        if 'SME Electricity' in linked_utilities:
            formatted_response += "\n\n**SME Electricity:**"
            details = linked_utilities['SME Electricity']
            if isinstance(details, str):
                formatted_response += f"\n- NMI: {details}"
            elif isinstance(details, bool) and details:
                formatted_response += "\n- Status: In File"
            if 'SME Electricity' in utility_retailers:
                retailers = utility_retailers['SME Electricity']
                if isinstance(retailers, list):
                    formatted_response += f"\n- Retailer: {', '.join(retailers)}"
                else:
                    formatted_response += f"\n- Retailer: {retailers}"

        # Handle C&I Gas
        if 'C&I Gas' in linked_utilities:
            formatted_response += "\n\n**C&I Gas:**"
            details = linked_utilities['C&I Gas']
            if isinstance(details, str):
                formatted_response += f"\n- MRIN: {details}"
            elif isinstance(details, bool) and details:
                formatted_response += "\n- Status: In File"
            if 'C&I Gas' in utility_retailers:
                retailers = utility_retailers['C&I Gas']
                if isinstance(retailers, list):
                    formatted_response += f"\n- Retailer: {', '.join(retailers)}"
                else:
                    formatted_response += f"\n- Retailer: {retailers}"

        # Handle SME Gas
        if 'SME Gas' in linked_utilities:
            formatted_response += "\n\n**SME Gas:**"
            details = linked_utilities['SME Gas']
            if isinstance(details, str):
                formatted_response += f"\n- MRIN: {details}"
            elif isinstance(details, bool) and details:
                formatted_response += "\n- Status: In File"
            if 'SME Gas' in utility_retailers:
                retailers = utility_retailers['SME Gas']
                if isinstance(retailers, list):
                    formatted_response += f"\n- Retailer: {', '.join(retailers)}"
                else:
                    formatted_response += f"\n- Retailer: {retailers}"

        # Handle Small Gas (alternative name for SME Gas)
        if 'Small Gas' in linked_utilities:
            formatted_response += "\n\n**Small Gas:**"
            details = linked_utilities['Small Gas']
            if isinstance(details, str):
                formatted_response += f"\n- MRIN: {details}"
            elif isinstance(details, bool) and details:
                formatted_response += "\n- Status: In File"
            if 'Small Gas' in utility_retailers:
                retailers = utility_retailers['Small Gas']
                if isinstance(retailers, list):
                    formatted_response += f"\n- Retailer: {', '.join(retailers)}"
                else:
                    formatted_response += f"\n- Retailer: {retailers}"

        # Handle Waste
        if 'Waste' in linked_utilities:
            formatted_response += "\n\n**Waste:**"
            details = linked_utilities['Waste']
            if isinstance(details, str):
                formatted_response += f"\n- Account Number: {details}"
            elif isinstance(details, bool) and details:
                formatted_response += "\n- Status: In File"
            if 'Waste' in utility_retailers:
                retailers = utility_retailers['Waste']
                if isinstance(retailers, list):
                    formatted_response += f"\n- Retailer: {', '.join(retailers)}"
                else:
                    formatted_response += f"\n- Retailer: {retailers}"

        # Handle Oil (formatted like other utilities)
        if 'Oil' in linked_utilities:
            formatted_response += "\n\n**Oil:**"
            details = linked_utilities['Oil']
            if isinstance(details, str):
                formatted_response += f"\n- Account Name: {details}"
            elif isinstance(details, list):
                formatted_response += f"\n- Account Name: {', '.join(details)}"
            elif isinstance(details, bool) and details:
                formatted_response += f"\n- Status: In File"
            if 'Oil' in utility_retailers:
                retailers = utility_retailers['Oil']
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
        
        # Process file IDs for easy access by other functions
        
        # Process business documents
        for doc_name, status in business_documents.items():
            if status:
                file_id_key = doc_name.replace(" (Exit Map)", "")
                if file_id_key == "Site Profling":
                    file_id_key = "Site Profiling"
                
                file_link = file_ids_dict.get(f"{file_id_key} File ID Link")
                if not file_link:
                    file_id = file_ids_dict.get(file_id_key)
                    if file_id:
                        file_link = f"https://drive.google.com/file/d/{file_id}/view"
                
                if file_link:
                    processed_file_ids[f"business_{doc_name}"] = file_link
        
        # Process signed contracts
        for sc_key, sc_label in sc_fields:
            sc_file_id = file_ids_dict.get(sc_key)
            if sc_file_id:
                sc_link = f"https://drive.google.com/file/d/{sc_file_id}/view"
                processed_file_ids[f"contract_{sc_label}"] = sc_link
        
        # Return both the raw data and formatted output, plus processed file IDs
        return {
            **data,  # Include all raw data
            "_formatted_output": formatted_response,  # Add formatted output
            "_processed_file_ids": processed_file_ids  # Add processed file IDs for other functions
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error in get_business_information: {str(e)}", exc_info=True)
        return {"_formatted_output": "Error: Unable to connect to the server. Please try again later."}
    except Exception as e:
        logger.error(f"Unexpected error in get_business_information: {str(e)}", exc_info=True)
        return {"_formatted_output": "Error: An unexpected error occurred. Please try again later."}