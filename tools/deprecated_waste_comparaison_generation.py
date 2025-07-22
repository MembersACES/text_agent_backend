from langchain_core.tools import tool
import requests
import pandas as pd
from openai import OpenAI
import json
import logging


@tool
def waste_comparaison_generation(
    comparison_spreadsheet_file_path: str,
    service_agreement_pdf_file_path: str,
) -> str:
    """
    Generate a waste comparison document. You will use this only when the user has uploaded the Excel file.

    Args:
        comparison_spreadsheet_file_path: The path to the uploaded spreadsheet file containing the comparison data
        service_agreement_pdf_file_path: The path to the uploaded PDF file containing the service agreement

    Returns:
        str: Response message indicating success or failure of sending the report
    """

    # Update logging configuration to force output
    logging.getLogger().handlers = []  # Clear existing handlers
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True,  # Force override any existing logging configuration
    )
    logger = logging.getLogger(__name__)

    # Add a direct console handler to be extra sure
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logger.debug("Starting waste savings report email creation")

    try:
        logger.debug(f"Reading Excel file from: {comparison_spreadsheet_file_path}")
        dfs = pd.read_excel(
            comparison_spreadsheet_file_path,
            sheet_name=None,
            engine="openpyxl",
            header=None,
            skiprows=0,
            na_filter=False,
        )
        logger.debug(f"Successfully read Excel file. Found {len(dfs)} sheets")

        excel_context = f"Filename: {comparison_spreadsheet_file_path}\n\nExcel file contains the following sheets:\n\n"
        for sheet_name, df in dfs.items():
            df.fillna("", inplace=True)
            df = df.astype(str).apply(lambda x: x.str.strip())
            dfs[sheet_name] = df

            excel_context += f"Sheet: {sheet_name}\n"
            excel_context += (
                f"- Columns: {[str(col).strip() for col in df.columns.tolist()]}\n"
            )
            excel_context += f"- Content:\n{df.to_string(index=False).strip()}\n\n"

        client = OpenAI()

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": """You are a helpful assistant that analyzes Excel data to find key financial information.
                    Your task is to carefully analyze the spreadsheet content and extract the following information:

                    1. Find the total savings amount (usually comparing old vs new costs or shown as a savings figure)
                    2. Look for any contract end date or service period end date
                    3. Identify monthly spending/cost figures
                    4. Find annual spending/cost figures (both current/old and new/proposed)
                    5. Identify landfill diversion percentages or amounts (from and to)
                    6. Extract the business/client name from either the filename or within the spreadsheet content

                    Important notes:
                    - The information might be in any sheet and may use different column names
                    - Numbers might be formatted with currency symbols (£, $) and thousands separators
                    - Veolia is NEVER the name of the business - it's the service provider
                    - If you can't find a specific value, use "TBC" for dates and "0" for numbers

                    Return ONLY a raw JSON object in exactly this format:
                    {
                        "business_name": "text"
                        "current_estimated_annual_spend": "number as text",
                        "veolia_offer_estimated_annual_spend": "number as text",
                        "option1_annual_landfill_tonnes": "number as text",
                        "option1_annual_diversion_from_landfill_tonnes": "number as text",
                        "option1_annual_diversion_percentage": "percentage as text",
                        "veolia_offer_annual_landfill_tonnes": "number as text",
                        "veolia_annual_diversion_from_landfill_tonnes": "number as text",
                        "veolia_offer_annual_diversion_percentage": "percentage as text",
                    }
                    Do not include any other text, markdown formatting, or code block indicators.""",
                },
                {
                    "role": "user",
                    "content": f"Please analyze this Excel data and extract the requested information:\n\n{excel_context}",
                },
            ],
        )

        # Log the complete response object
        logger.debug(f"Complete OpenAI response object: {response}")

        # Parse the JSON response
        raw_content = response.choices[0].message.content
        logger.debug(f"Raw content before JSON parsing: {raw_content}")
        try:
            response_data = json.loads(raw_content)
            logger.debug(f"Parsed JSON data: {response_data}")
        except json.JSONDecodeError as json_err:
            logger.error(f"JSON parsing error: {json_err}")
            return f"Error parsing JSON response: {str(json_err)}"

        payload = {
            "business_name": response_data.get("business_name", ""),
            "current_estimated_annual_spend": response_data.get(
                "current_estimated_annual_spend", ""
            ),
            "veolia_offer_estimated_annual_spend": response_data.get(
                "veolia_offer_estimated_annual_spend", ""
            ),
            "option1_annual_landfill_tonnes": response_data.get(
                "option1_annual_landfill_tonnes", ""
            ),
            "option1_annual_diversion_from_landfill_tonnes": response_data.get(
                "option1_annual_diversion_from_landfill_tonnes", ""
            ),
            "option1_annual_diversion_percentage": response_data.get(
                "option1_annual_diversion_percentage", ""
            ),
            "veolia_offer_annual_landfill_tonnes": response_data.get(
                "veolia_offer_annual_landfill_tonnes", ""
            ),
            "veolia_annual_diversion_from_landfill_tonnes": response_data.get(
                "veolia_annual_diversion_from_landfill_tonnes", ""
            ),
            "veolia_offer_annual_diversion_percentage": response_data.get(
                "veolia_offer_annual_diversion_percentage", ""
            ),
        }

        files = {
            "comparison_spreadsheet": (
                "comparison_spreadsheet.xlsx",
                open(comparison_spreadsheet_file_path, "rb"),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
            "service_agreement_pdf": (
                "service_agreement.pdf",
                open(service_agreement_pdf_file_path, "rb"),
                "application/pdf",
            ),
        }

        response = requests.post(
            "https://membersaces.app.n8n.cloud/webhook/create-waste-savings-report",
            data=payload,
            files=files,
        )

        if response.status_code != 200:
            return "Sorry, there was an error sending the waste savings report."

        data = response.json()
        return f"Successfully generated the waste comparison"
    except Exception as e:
        return f"Error processing the spreadsheet file: {str(e)}"
