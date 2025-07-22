import json
import requests
from datetime import datetime
from typing import Dict, Optional, Any
from langchain.tools import tool
import os

@tool
def site_profiling(file_path: str, business_name: str, business_info: dict = None, questionnaire_data: dict = None) -> str:
    """Process site profiling data and send to n8n webhook"""
    try:
        # If business_info is None, try to get it from the message content
        if business_info is None:
            return "Error: Business information is required"
            
        # Get record ID from business info
        record_id = business_info.get('record_ID', '')
        if not record_id:
            return "Error: Record ID not found in business information"
            
        # Check if this is an interactive questionnaire (no file)
        if file_path == "" or file_path is None:
            # This is interactive questionnaire data - use NEW webhook
            webhook_data = {
                "business_name": business_name,
                "business_info": business_info,
                "questionnaire_data": questionnaire_data,
                "record_ID": record_id,
                "timestamp": datetime.now().isoformat(),
                "source": "interactive_questionnaire"
            }
            
            response = requests.post(
                'https://membersaces.app.n8n.cloud/webhook/new_site_profiling',
                json=webhook_data,
                headers={'Accept': 'application/json'}
            )
            
            if response.status_code == 200:
                try:
                    response_data = response.json()
                    status = response_data.get('status', '')
                    site_profiling_id = response_data.get('site_profiling_ID', '')
                    
                    if status and site_profiling_id:
                        # Create Google Sheet link
                        sheet_link = f"https://docs.google.com/spreadsheets/d/{site_profiling_id}"
                        return f"✅ {status}\n\nYou can access the site profiling document here: [View Site Profiling]({sheet_link})"
                    else:
                        return f"Successfully processed interactive site profiling data for {business_name}"
                except json.JSONDecodeError:
                    return f"Successfully processed interactive site profiling data for {business_name}"
            else:
                return f"Error: Failed to process interactive site profiling data. Status code: {response.status_code}, Response: {response.text}"
        
        # This is a file upload - use existing file upload webhook
        # Get the full Google Drive URL
        gdrive_url = business_info.get('gdrive', {}).get('folder_url', '')
        if not gdrive_url:
            return "Error: Could not find Google Drive folder URL in business information"
            
        # Read the file in binary mode
        with open(file_path, 'rb') as f:
            file_content = f.read()
            
        # Get the filename from the path
        filename = os.path.basename(file_path)
        
        # Prepare the files for multipart form request
        files = {
            'file': (filename, file_content, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        }
        
        # Get signed_date from business_info
        signed_date = business_info.get('representative_details', {}).get('signed_date', '')
        
        # Prepare the form data
        data = {
            'business_name': business_name,
            'business_info': json.dumps(business_info),
            'gdrive_url': gdrive_url,
            'signed_date': signed_date,
            'record_ID': record_id,
            'timestamp': datetime.now().isoformat()
        }
        
        # Send to n8n webhook with proper headers
        headers = {
            'Accept': 'application/json'
        }
        
        response = requests.post(
            'https://membersaces.app.n8n.cloud/webhook/site_profiling/',
            files=files,
            data=data,
            headers=headers
        )
        
        if response.status_code == 200:
            try:
                response_data = response.json()
                status = response_data.get('status', '')
                site_profiling_id = response_data.get('site_profiling_ID', '')
                
                if status and site_profiling_id:
                    # Create Google Sheet link
                    sheet_link = f"https://docs.google.com/spreadsheets/d/{site_profiling_id}"
                    return f"✅ {status}\n\nYou can access the site profiling document here: [View Site Profiling]({sheet_link})"
                else:
                    return f"Successfully processed site profiling data for {business_name}"
            except json.JSONDecodeError:
                return f"Successfully processed site profiling data for {business_name}"
        else:
            return f"Error: Failed to process site profiling data. Status code: {response.status_code}, Response: {response.text}"
            
    except Exception as e:
        return f"Error processing site profiling data: {str(e)}" 