"""Seed defaults for operational email templates and recipient lists.

Copied from the current hardcoded sender maps so first boot matches today's
behaviour. Existing DB rows are never overwritten by seed.
"""

from __future__ import annotations

import re

from tools.send_supplier_signed_agreement import CONTRACT_EMAIL_MAPPINGS, EOI_EMAIL_MAPPINGS
from tools.supplier_data_request import EMAIL_TEMPLATES, RETAILER_EMAILS
from tools.supplier_quote_request import QUOTE_RETAILER_EMAILS

_FORMAT_TOKEN_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")

PLACEHOLDER_EMAIL_DOMAINS = frozenset(
    {
        "supplier.com",
        "partner.com",
        "recycler.com",
        "trading.com",
        "management.com",
        "scheme.com",
    }
)

CONTRACT_GROUPS: dict[str, str] = {
    "PowerMetric DMA": "DMA",
    "Origin C&I Electricity": "C&I Electricity",
    "Origin SME Electricity": "SME Electricity",
    "BlueNRG SME Electricity": "SME Electricity",
    "Origin C&I Gas": "C&I Gas",
    "Momentum C&I Electricity": "C&I Electricity",
    "CovaU SME Gas": "SME Gas",
    "CovaU SME Electricity": "SME Electricity",
    "Veolia Waste": "Waste",
    "Alinta C&I Electricity": "C&I Electricity",
    "Alinta C&I Gas": "C&I Gas",
    "Other": "Other",
}

EOI_GROUPS: dict[str, str] = {
    "Direct Meter Agreement": "Energy Solutions",
    "Cleaning Robot": "Technology",
    "Inbound Digital Voice Agent": "Technology",
    "Cooking Oil Used Oil": "Waste Management",
    "Referral Distribution Program": "Business Services",
    "Solar Energy PPA": "Energy Solutions",
    "Self Managed Certificates": "Energy Solutions",
    "Telecommunication": "Technology",
    "Wood Pallet": "Business Services",
    "Wood Cut": "Business Services",
    "Baled Cardboard": "Waste Management",
    "Loose Cardboard": "Waste Management",
    "Large Generation Certificates Trading": "Energy Solutions",
    "GHG Action Plan": "Environmental",
    "Government Incentives Vic G4": "Environmental",
    "Self Managed VEECs": "Energy Solutions",
    "Demand Response": "Energy Solutions",
    "Waste Organic Recycling": "Waste Management",
    "Waste Grease Trap": "Waste Management",
    "Used Wax Cardboard": "Waste Management",
    "Vic CDS Scheme": "Waste Management",
    "New Placeholder Template": "Templates",
}

QUOTE_GROUPS: dict[str, str] = {
    "Test": "all",
    "Data Quote": "all",
    "Origin C&I": "electricity_ci,gas_ci",
    "Alinta C&I": "electricity_ci,gas_ci",
    "Shell C&I": "electricity_ci,gas_ci",
    "Momentum C&I": "electricity_ci,gas_ci",
    "Origin SME": "electricity_sme,gas_sme",
    "Alinta SME": "electricity_sme,gas_sme",
    "Shell SME": "electricity_sme,gas_sme",
    "Momentum SME": "electricity_sme,gas_sme",
    "Waste Provider 1": "waste",
    "Waste Provider 2": "waste",
    "Oil Provider 1": "oil",
    "Oil Provider 2": "oil",
}

DATA_REQUEST_GROUPS: dict[str, str] = {
    "Origin C&I Electricity": "C&I Electricity",
    "Origin C&I Gas": "C&I Gas",
    "Momentum C&I Electricity": "C&I Electricity",
    "Alinta C&I Electricity": "C&I Electricity",
    "Alinta C&I Gas": "C&I Gas",
    "Alinta C&I Electricity & Gas": "C&I Electricity",
    "Energy Australia C&I E & G": "C&I Electricity",
    "AGL C&I E & G": "C&I Electricity",
    "Shell Energy": "C&I Electricity",
    "BlueNRG SME Electricity": "SME Electricity",
    "CovaU SME Electricity": "SME Electricity",
    "CovaU SME": "SME Electricity",
    "Origin SME": "SME Electricity",
    "Momentum SME": "SME Electricity",
    "Next Business Energy SME": "SME Electricity",
    "1st Energy SME": "SME Electricity",
    "Red Energy SME": "SME Electricity",
    "GloBird Energy SME": "SME Electricity",
    "Powerdirect SME": "SME Electricity",
    "Sumo SME": "SME Electricity",
    "Tango Energy": "SME Electricity",
    "Sun Retail": "SME Electricity",
    "Ergon Energy": "SME Electricity",
    "Veolia Waste": "Waste",
    "Other": "Other",
}

DATA_REQUEST_SAMPLE = {
    "business_name": "Example Pty Ltd",
    "nmi": "4102000000",
    "mrin": "5300000000",
    "account_number": "ACC-12345",
}

QUOTE_HTML = """<!DOCTYPE html>
<html>
<head>
  <base target="_top">
</head>
<body>
  <p>Hello Team, </p>
  <p>I hope this email finds you well. </p>
  <p>I am requesting a {{request_kind_label}} for my client {{business_name}} {{identifier_display}}. Can you please provide a quote with the provided information below:</p>
  <ul>
    <li>Start Date - {{start_date}}</li>
    <li>Contract Options - {{quote_details}} </li>
    <li>Commission - {{commission}} </li>
    <li>Current Retailer - {{current_retailer}} </li>
    <li>Offer Due - {{offer_due}}</li>
  </ul>
    <p>Please Find Below the member's details:</p>
  <ul>
    <li>Company Name  - {{business_name}} </li>
    <li>Trading as -  {{trading_as}} </li>
    <li>ABN - {{abn}} </li>
    <li>Site Address - {{site_address}} </li>
    <li>Contact Name - {{client_name}} </li>
    <li>Number - {{client_number}} </li>
    <li>Email Address - {{client_email}} </li>
    <li>{{identifier_display}} </li>
  </ul>
{{consumption_html}}
{{attachments_html}}
  <p>Please let me know if you have any questions or concerns.</p>
  <p>Kind Regards,</p>
  <p>Alice </p>
  <p>ForNRG Team </p>
  <p> FORNRG Pty Ltd </p>
  <p> P: 1300 440 224 </p>
  <p> W: http://www.fornrg.com/ </p>
  <p> NOTE: This email, including any attachments, is strictly confidential. If you received this email in error, please notify the sender and delete it as well as any copies from your system. You must not use, print, distribute, copy, or disclose the content of this email if you are not the intended recipient. </p>
</body>
</html>"""

SIGNED_AGREEMENT_HTML = """<!DOCTYPE html>
<html>
<head>
  <base target="_top">
</head>
<body>
  <p>Hello Team,</p>
  <p>I hope this email finds you well.</p>
  <p>Please find attached the signed {{agreement_label}} for our member <strong>{{business_name}}</strong>.</p>
  <p>Document type: {{contract_type}}</p>
  {{identifier_html}}
  <p>Please let me know if you have any questions or concerns.</p>
  <p>Kind regards,</p>
  <p>Alice</p>
  <p>ForNRG Team<br>
  FORNRG Pty Ltd<br>
  P: 1300 440 224<br>
  W: <a href="http://www.fornrg.com/">http://www.fornrg.com/</a></p>
</body>
</html>"""

ALINTA_HTML = """<!DOCTYPE html>
<html>
<head>
  <base target="_top">
</head>
<body>
  <p>Hello Team,</p>
  <p>I hope this email finds you well.</p>
  <p>This is an Agreement Request for our member, {{company_name}} (MIRN {{mirn}}).</p>
  {{request_kind_html}}
  <p>Company Name: {{company_name}}</p>
  <p>ACN/ABN:{{acn_abn}}<br>
  Address: {{address}}<br>
  Tel: {{tel}}<br>
  Contact Name: {{contact_name}}<br>
  Email: {{email}}</p>
  <p>Period</p>
  <p>Start date:{{start_date}}<br>
  End date: {{end_date}}<br>
  Price per GJ: {{price_per_gj}}<br>
  Commission: {{commission_per_gj}}</p>
  <p>Conditions:<br>
  Contract Period Quantity (GJ) {{cpq_gj}}<br>
  Minimum Contract Period Quantity (GJ) {{min_cpq_gj}}<br>
  Minimum Contract Period Quantity (%of CPQ) {{min_cpq_pct}}<br>
  Contract Maximum Daily Quantity (GJ) {{mdq_gj}}</p>
  <p>Attached are both the LOA &amp; the signed engagement form.</p>
  <p>Kind regards,</p>
  <p>Alice</p>
  <p>FORNRG Pty Ltd<br>
  1300 938 638<br>
  W: <a href="http://www.fornrg.com/">http://www.fornrg.com/</a></p>
</body>
</html>"""

SHARE_FOLDER_HTML = """<!DOCTYPE html>
<html>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,Helvetica,sans-serif;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f3f4f6;padding:24px 12px;">
    <tr>
      <td align="center">
        <table role="presentation" width="600" cellspacing="0" cellpadding="0" style="max-width:600px;background:#ffffff;border-radius:12px;overflow:hidden;border:1px solid #e5e7eb;">
          <tr>
            <td style="background:#5750F1;padding:20px 28px;">
              <p style="margin:0;font-size:18px;font-weight:700;color:#ffffff;">Carbon Zero Australasia</p>
            </td>
          </tr>
          <tr>
            <td style="padding:28px;">
              <p style="margin:0 0 12px;font-size:16px;color:#111827;">Hello,</p>
              <p style="margin:0 0 16px;font-size:15px;line-height:1.55;color:#374151;">
                Carbon Zero Australasia has shared documents with you for <strong>{{business_name}}</strong>.
                You can open the folder below (Google account required).
              </p>
              <p style="margin:0 0 20px;">
                <a href="{{folder_url}}" style="display:inline-block;background:#5750F1;color:#ffffff;text-decoration:none;font-size:14px;font-weight:700;padding:12px 18px;border-radius:8px;">
                  Open shared documents
                </a>
              </p>
              {{files_html}}
              <p style="margin:0 0 16px;font-size:13px;line-height:1.5;color:#6b7280;">
                If the button does not work, copy this link:<br>
                <a href="{{folder_url}}" style="color:#5750F1;word-break:break-all;">{{folder_url}}</a>
              </p>
              <p style="margin:0;font-size:14px;line-height:1.55;color:#374151;">
                Kind regards,<br>
                {{signoff_html}}<br>
                Carbon Zero Australasia
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


def python_format_to_tokens(text: str) -> str:
    return _FORMAT_TOKEN_RE.sub(r"{{\1}}", text)


def split_emails(raw: str) -> list[str]:
    return [part.strip() for part in (raw or "").split(",") if part.strip()]


def emails_look_placeholder(emails: list[str], key: str = "") -> bool:
    if key in {"New Placeholder Template"}:
        return True
    if key in {"Waste Provider 1", "Waste Provider 2", "Oil Provider 1", "Oil Provider 2", "Shell SME", "Momentum SME"}:
        return True
    for email in emails:
        domain = email.rsplit("@", 1)[-1].lower() if "@" in email else ""
        if domain in PLACEHOLDER_EMAIL_DOMAINS:
            return True
    return False


def data_request_templates() -> list[dict]:
    names = {
        "electricity_ci_data": "C&I Electricity data request",
        "electricity_sme_data": "SME Electricity data request",
        "gas_ci_data": "C&I Gas data request",
        "gas_sme_data": "SME Gas data request",
        "waste_data": "Waste data request",
    }
    descriptions = {
        "electricity_ci_data": "Sent when requesting interval data and contract details from a C&I electricity retailer.",
        "electricity_sme_data": "Sent when requesting SME electricity invoices and contract end date.",
        "gas_ci_data": "Sent when requesting C&I gas interval data, invoices, and contract copy.",
        "gas_sme_data": "Sent when requesting SME gas invoices and contract end date.",
        "waste_data": "Sent when requesting waste invoices, bin weights, and recycling confirmation.",
    }
    fields = {
        "electricity_ci_data": ["business_name", "nmi"],
        "electricity_sme_data": ["business_name", "nmi"],
        "gas_ci_data": ["business_name", "mrin"],
        "gas_sme_data": ["business_name", "mrin"],
        "waste_data": ["business_name", "account_number"],
    }
    rows = []
    for key, config in EMAIL_TEMPLATES.items():
        rows.append(
            {
                "key": f"data_request.{key.removesuffix('_data')}",
                "category": "data_request",
                "name": names.get(key, key),
                "description": descriptions.get(key, ""),
                "subject": python_format_to_tokens(config["subject"]),
                "html_body": python_format_to_tokens(config["template"]),
                "merge_fields": fields.get(key, ["business_name"]),
                "sample_values": {field: DATA_REQUEST_SAMPLE.get(field, "Example") for field in fields.get(key, ["business_name"])},
            }
        )
    return rows


def extra_templates() -> list[dict]:
    return [
        {
            "key": "quote_request.default",
            "category": "quote_request",
            "name": "Supplier quote request",
            "description": "Used for quote requests and blend & extend. {{consumption_html}} and {{attachments_html}} are filled in automatically from the form.",
            "subject": "{{subject_prefix}} - {{business_name}} - {{identifier_display}}",
            "html_body": QUOTE_HTML,
            "merge_fields": [
                "subject_prefix",
                "request_kind_label",
                "business_name",
                "identifier_display",
                "start_date",
                "quote_details",
                "commission",
                "current_retailer",
                "offer_due",
                "trading_as",
                "abn",
                "site_address",
                "client_name",
                "client_number",
                "client_email",
                "consumption_html",
                "attachments_html",
            ],
            "sample_values": {
                "subject_prefix": "Quote Request",
                "request_kind_label": "quote",
                "business_name": "Example Pty Ltd",
                "identifier_display": "NMI - 4102000000",
                "start_date": "01/10/2026",
                "quote_details": "3 Year Stepped",
                "commission": "3%",
                "current_retailer": "Origin C&I",
                "offer_due": "15/10/2026",
                "trading_as": "Example",
                "abn": "12 345 678 901",
                "site_address": "1 Example St, Melbourne VIC 3000",
                "client_name": "Jane Smith",
                "client_number": "0400 000 000",
                "client_email": "jane@example.com",
                "consumption_html": "<p>Please Find Below the Site details:</p><ul><li>Total Yearly Consumption est  - 1,200,000 kWh </li></ul>",
                "attachments_html": "<p>Please see attached:</p><ul><li> The Letter of Authority </li><li> Copy of recent invoice </li></ul>",
            },
        },
        {
            "key": "signed_agreement.default",
            "category": "signed_agreement",
            "name": "Signed agreement lodgement",
            "description": "Sent to the supplier when a signed contract or EOI is lodged. Recipients are configured per contract type.",
            "subject": "Signed {{agreement_label}} - {{business_name}} - {{contract_type}}",
            "html_body": SIGNED_AGREEMENT_HTML,
            "merge_fields": [
                "business_name",
                "contract_type",
                "agreement_label",
                "identifier_html",
            ],
            "sample_values": {
                "business_name": "Example Pty Ltd",
                "contract_type": "Alinta C&I Electricity",
                "agreement_label": "contract",
                "identifier_html": "<p>NMI: 4102000000</p>",
            },
        },
        {
            "key": "alinta_gas.default",
            "category": "alinta_gas",
            "name": "Alinta C&I gas agreement request",
            "description": "Agreement request email built from the Alinta EF form.",
            "subject": "Agreement Request: G-C&I (GJ) {{cpq_gj}} {{request_kind}} {{company_name}} MIRN {{mirn}}",
            "html_body": ALINTA_HTML,
            "merge_fields": [
                "company_name",
                "mirn",
                "request_kind",
                "request_kind_html",
                "acn_abn",
                "address",
                "tel",
                "contact_name",
                "email",
                "start_date",
                "end_date",
                "price_per_gj",
                "commission_per_gj",
                "cpq_gj",
                "min_cpq_gj",
                "min_cpq_pct",
                "mdq_gj",
            ],
            "sample_values": {
                "company_name": "Example Pty Ltd",
                "mirn": "5300000000",
                "request_kind": "Retention",
                "request_kind_html": "<p>Please note this is a retention account.</p>",
                "acn_abn": "12 345 678 901",
                "address": "1 Example St, Melbourne VIC 3000",
                "tel": "03 9000 0000",
                "contact_name": "Jane Smith",
                "email": "jane@example.com",
                "start_date": "01/10/2026",
                "end_date": "30/09/2029",
                "price_per_gj": "$12.50",
                "commission_per_gj": "$0.50",
                "cpq_gj": "10000",
                "min_cpq_gj": "8000",
                "min_cpq_pct": "80%",
                "mdq_gj": "50",
            },
        },
        {
            "key": "share_folder.default",
            "category": "share_folder",
            "name": "Shared folder notification",
            "description": "Sent when staff share a member Drive folder. {{files_html}} and {{signoff_html}} are filled in automatically.",
            "subject": "Carbon Zero Australasia has shared documents with you — {{business_name}}",
            "html_body": SHARE_FOLDER_HTML,
            "merge_fields": ["business_name", "folder_url", "files_html", "signoff_html"],
            "sample_values": {
                "business_name": "Example Pty Ltd",
                "folder_url": "https://drive.google.com/drive/folders/example",
                "files_html": "<p style=\"margin:16px 0 8px;font-size:14px;color:#374151;\">Documents included:</p><ul style=\"margin:0 0 16px;padding-left:18px;font-size:14px;color:#111827;\"><li style=\"margin:0 0 6px;\">Quote.pdf</li></ul>",
                "signoff_html": "Alex Example<br>alex@acesolutions.com.au",
            },
        },
    ]


def all_templates() -> list[dict]:
    return data_request_templates() + extra_templates()


def _recipient_row(
    flow: str,
    key: str,
    display_name: str,
    emails: list[str],
    group_name: str,
    aliases: list[str] | None = None,
    extra_groups: list[str] | None = None,
    sort_order: int = 0,
) -> dict:
    return {
        "flow": flow,
        "key": key,
        "display_name": display_name,
        "emails": emails,
        "aliases": aliases or [],
        "group_name": group_name,
        "extra_groups": extra_groups or [],
        "is_placeholder": emails_look_placeholder(emails, key),
        "sort_order": sort_order,
    }


def all_recipients() -> list[dict]:
    rows: list[dict] = []
    for index, (key, config) in enumerate(RETAILER_EMAILS.items()):
        rows.append(
            _recipient_row(
                flow="data_request",
                key=key,
                display_name=str(config["name"]),
                emails=split_emails(str(config["email"])),
                group_name=DATA_REQUEST_GROUPS.get(key, "Other"),
                aliases=list(config.get("variants") or []),
                sort_order=index * 10,
            )
        )
    for index, (key, config) in enumerate(QUOTE_RETAILER_EMAILS.items()):
        rows.append(
            _recipient_row(
                flow="quote_request",
                key=key,
                display_name=str(config["name"]),
                emails=split_emails(str(config["email"])),
                group_name=QUOTE_GROUPS.get(key, "all"),
                aliases=list(config.get("variants") or []),
                sort_order=index * 10,
            )
        )
    extra_quote = [
        ("Shell SME", "Shell SME", ["data.quote@fornrg.com"], "electricity_sme,gas_sme"),
        ("Momentum SME", "Momentum SME", ["data.quote@fornrg.com"], "electricity_sme,gas_sme"),
    ]
    existing_quote_keys = {row["key"] for row in rows if row["flow"] == "quote_request"}
    for offset, (key, name, emails, group) in enumerate(extra_quote):
        if key in existing_quote_keys:
            continue
        rows.append(
            _recipient_row(
                flow="quote_request",
                key=key,
                display_name=name,
                emails=emails,
                group_name=group,
                sort_order=1000 + offset,
            )
        )
    for index, (key, config) in enumerate(CONTRACT_EMAIL_MAPPINGS.items()):
        extra = (
            ["C&I Electricity", "SME Electricity", "C&I Gas", "SME Gas", "Waste", "DMA"]
            if key == "Other"
            else []
        )
        rows.append(
            _recipient_row(
                flow="signed_contract",
                key=key,
                display_name=str(config["name"]),
                emails=split_emails(str(config["email"])),
                group_name=CONTRACT_GROUPS.get(key, "Other"),
                extra_groups=extra,
                sort_order=index * 10,
            )
        )
    for index, (key, config) in enumerate(EOI_EMAIL_MAPPINGS.items()):
        extra = (
            [
                "Energy Solutions",
                "Waste Management",
                "Technology",
                "Business Services",
                "Environmental",
                "Templates",
            ]
            if key == "Other"
            else []
        )
        rows.append(
            _recipient_row(
                flow="eoi",
                key=key,
                display_name=str(config["name"]),
                emails=split_emails(str(config["email"])),
                group_name=EOI_GROUPS.get(key, "Other"),
                extra_groups=extra,
                sort_order=index * 10,
            )
        )
    rows.append(
        _recipient_row(
            flow="alinta_gas",
            key="default",
            display_name="Alinta gas agreement request",
            emails=["data.quote@fornrg.com"],
            group_name="Alinta",
        )
    )
    return rows
