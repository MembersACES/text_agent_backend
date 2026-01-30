# One Month Savings Invoice - Direct Google Sheets Integration Setup

This guide explains how to configure the One Month Savings Invoice feature to use Google Sheets API directly (bypassing n8n).

## Environment Variables

You need to set these environment variables in your backend:

### Required:
- `ONE_MONTH_SAVINGS_SHEET_ID` - The Google Sheet ID where invoices are stored
  - You can find this in the Google Sheets URL: `https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit`
  
- `ONE_MONTH_SAVINGS_SHEET_NAME` - The name of the sheet tab (default: "Sheet1")
  - This is the tab name at the bottom of your Google Sheet

### Service Account Credentials:

You have two options:

#### Option 1: Service Account JSON File (Local Development)
- Place your service account JSON file in the backend root directory
- Name it `service-account-key.json` (or set `SERVICE_ACCOUNT_FILE` env var to your filename)
- The service account needs access to the Google Sheet

#### Option 2: Service Account JSON as Environment Variable (Cloud Run/Production)
- Set `SERVICE_ACCOUNT_JSON` environment variable with the full JSON content
- This is better for Cloud Run deployments where you can't easily place files

### Optional:
- `USE_N8N_FALLBACK` - Set to "true" to fallback to n8n if Google Sheets API fails (default: "false")
- `SERVICE_ACCOUNT_FILE` - Path to service account JSON file (default: "service-account-key.json")

## Google Sheet Structure

Your Google Sheet should have these columns (in order):
1. Invoice Number
2. Business Name (Member)
3. Business ABN
4. Contact Name
5. Contact Email
6. Invoice Date
7. Due Date
8. Services (summary of line items)
9. Subtotal
10. GST
11. Total Amount
12. Status
13. Created At
14. Line Items JSON (full line items as JSON string)

## Setting Up Service Account

1. **Create a Service Account** (if you don't have one):
   - Go to Google Cloud Console → IAM & Admin → Service Accounts
   - Create a new service account
   - Download the JSON key file

2. **Share the Google Sheet with the Service Account**:
   - Open your Google Sheet
   - Click "Share" button
   - Add the service account email (found in the JSON file, e.g., `your-service@project.iam.gserviceaccount.com`)
   - Give it "Editor" permissions

3. **Enable Google Sheets API**:
   - Go to Google Cloud Console → APIs & Services → Library
   - Search for "Google Sheets API"
   - Click "Enable"

## Example .env File

```env
# Google Sheets Configuration
ONE_MONTH_SAVINGS_SHEET_ID=1mAV5_Efn8nYNAhO3AWy-WFl5qZqzolzP1-ttHeFlMaY
ONE_MONTH_SAVINGS_SHEET_NAME=Invoices

# Service Account (Option 1: File path)
SERVICE_ACCOUNT_FILE=service-account-key.json

# OR Service Account (Option 2: JSON as string for Cloud Run)
# SERVICE_ACCOUNT_JSON={"type":"service_account","project_id":"..."}

# Optional: Fallback to n8n if Sheets API fails
USE_N8N_FALLBACK=false
```

## Testing

Once configured, the system will:
1. Write invoices directly to Google Sheets (no n8n required)
2. Read invoice history directly from Google Sheets
3. Generate sequential invoice numbers by reading all existing invoice numbers

If `USE_N8N_FALLBACK=true` and the Google Sheets API fails, it will automatically fallback to the n8n webhook.

## Troubleshooting

- **"Could not connect to Google Sheets"**: Check that your service account JSON is valid and the sheet is shared with the service account email
- **"SHEET_ID not configured"**: Make sure `ONE_MONTH_SAVINGS_SHEET_ID` environment variable is set
- **Permission errors**: Ensure the service account has "Editor" access to the Google Sheet
- **Column mapping issues**: Verify your sheet columns match the expected structure above

