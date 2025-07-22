from langchain_core.tools import tool
import requests
from typing import Optional


@tool
def meeting_summary_generation(
    aces_team_member: str,
    business_member: str,
    meeting_format: str,
    meeting_date: str,
    key_points: str,
    next_actions: str,
    timeframe: str,
    member_profile_status: str,
) -> str:
    """Generate a meeting summary document.

    Args:
        aces_team_member: Name of the ACES team member who attended the meeting
        business_member: Name of the business member who attended the meeting
        meeting_format: Format of the meeting (face-to-face or online)
        meeting_date: Date of the meeting
        key_points: Key points discussed during the meeting
        next_actions: Next actions or follow-ups agreed upon
        timeframe: Timeframe for the next actions
        member_profile_status: Status of the member profile (Completed or Not Completed)
    """
    payload = {
        "aces_team_member": aces_team_member,
        "business_member": business_member,
        "meeting_format": meeting_format,
        "meeting_date": meeting_date,
        "key_points": key_points,
        "next_actions": next_actions,
        "timeframe": timeframe,
        "member_profile_status": member_profile_status,
    }

    response = requests.post(
        "https://membersaces.app.n8n.cloud/webhook/meeting_agent/send_email/ta",
        json=payload,
    )

    if response.status_code != 200:
        raise Exception(
            f"Failed to generate meeting summary. Status code: {response.status_code}"
        )

    data = response.json()
    pdf_document_link = data.get("pdf_document_link")
    spreadsheet_document_link = data.get("spreadsheet_document_link")

    return f"The Meeting Summary document has been successfully added."
