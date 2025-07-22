import json
import requests
from datetime import datetime
from langchain_core.tools import tool
from typing import Optional


@tool
def task_request(
    task_name: str,
    team_member: str,
    summary: str,
    action_required: str,
    due_date: str,
    drive_link: Optional[str] = None,
) -> str:
    """Process a task request and create a task with the provided information.

    Args:
        task_name: Name of the task (e.g., "Client Follow-Up", "Supplier Follow-Up")
        team_member: Team member assigned to the task (e.g., "Michelle", "Claire", "Jared")
        summary: Brief description of findings/status
        action_required: What needs to be done
        due_date: When the task needs to be completed by
        drive_link: Optional Google Drive link

    Returns:
        A confirmation message with the task details in the format:
        "✅ Task '[task_name]' created for [team_member], due on [due_date]:

        Task Report – [task_name]
        Date: [date]
        Team Member: [team_member]
        Summary: [summary]
        Action Required: [action_required]
        Due Date: [due_date]
        Drive Link: [drive_link] (if provided)"
    """
    task = {
        "task_name": task_name,
        "date": datetime.now().strftime("%d %B, %Y"),
        "team_member": team_member,
        "summary": summary,
        "action_required": action_required,
        "due_date": due_date,
        "drive_link": drive_link,
    }

    webhook_data = {
        "task_name": task["task_name"],
        "creation_date": task["date"],
        "team_member": task["team_member"],
        "summary": task["summary"],
        "action_required": task["action_required"],
        "due_date": task["due_date"],
        "drive_link": task.get("drive_link", ""),
        "email_subject": f"Task Report – {task['team_member']} – {task['task_name']}",
        "email_to": "members@acesolutions.com.au",
    }

    webhook_url = "https://membersaces.app.n8n.cloud/webhook/task_requests/"

    try:
        response = requests.post(
            webhook_url,
            headers={"Content-Type": "application/json"},
            data=json.dumps(webhook_data),
        )

        if response.status_code != 200:
            print(
                f"Failed to send task to n8n: {response.status_code} - {response.text}"
            )
    except Exception as e:
        print(f"Error sending task to n8n: {str(e)}")

    return f"✅ Task '{task['task_name']}' created for {task['team_member']}, due on {task['due_date']}:\n\n"
