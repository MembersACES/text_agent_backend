import requests
import logging
import re
import os
from typing import Dict, Tuple


try:
    from tools.drive_filing import drive_filing
    from tools.business_info import get_business_information
except ImportError:
    # Fallback for different import structures
    try:
        from .drive_filing import drive_filing
        from .business_info import get_business_information
    except ImportError:
        # If both fail, we'll handle it gracefully
        drive_filing = None
        get_business_information = None
        logging.warning("Could not import drive_filing or get_business_information functions")

logger = logging.getLogger(__name__)

# Contract email mappings
CONTRACT_EMAIL_MAPPINGS = {
    "PowerMetric DMA": {
        "name": "PowerMetric",
        "email": "accountmanagement@powermetric.com.au, rmorse@powermetric.com.au, data.quote@fornrg.com"
    },
    "Origin C&I Electricity": {
        "name": "Origin C&I",
        "email": "MIContracts@originenergy.com.au, data.quote@fornrg.com"
    },
    "Origin SME Electricity": {
        "name": "Origin SME",
        "email": "MIContracts@originenergy.com.au, data.quote@fornrg.com"
    },
    "BlueNRG SME Electricity": {
        "name": "BlueNRG SME",
        "email": "data.quote@fornrg.com"
    },
    "Origin C&I Gas": {
        "name": "Origin C&I",
        "email": "MIContracts@originenergy.com.au, data.quote@fornrg.com"
    },
    "Momentum C&I Electricity": {
        "name": "Momentum",
        "email": "contracts.administration@momentum.com.au, data.quote@fornrg.com"
    },
    "CovaU SME Gas": {
        "name": "CovaU",
        "email": "corp.sales@covau.com.au, data.quote@fornrg.com"
    },
    "CovaU SME Electricity": {
        "name": "CovaU",
        "email": "corp.sales@covau.com.au, data.quote@fornrg.com"
    },
    "Veolia Waste": {
        "name": "Veolia",
        "email": "ric.luiyf@veolia.com, business@acesolutions.com.au"
    },
    "Alinta C&I Electricity": {
        "name": "Alinta",
        "email": "Andrew.Barnes@alintaenergy.com.au, Lewis.Chase@alintaenergy.com.au, Cindy.Ho@alintaenergy.com.au, business@acesolutions.com.au, data.quote@fornrg.com"
    },
    "Alinta C&I Gas": {
        "name": "Alinta",
        "email": "Andrew.Barnes@alintaenergy.com.au, Lewis.Chase@alintaenergy.com.au, Cindy.Ho@alintaenergy.com.au, business@acesolutions.com.au, data.quote@fornrg.com"
    },
    "Other": {
        "name": "Other",
        "email": "members@acesolutions.com.au, data.quote@fornrg.com, morgan.h@acesolutions.com.au"
    },
}

# EOI email mappings
EOI_EMAIL_MAPPINGS = {
    "Direct Meter Agreement": {
        "name": "DMA Supplier",
        "email": "data.quote@fornrg.com"
    },
    "Cleaning Robot": {
        "name": "Cleaning Tech",
        "email": "cleantech@supplier.com"
    },
    "Inbound Digital Voice Agent": {
        "name": "Voice Tech",
        "email": "voicetech@supplier.com"
    },
    "Cooking Oil Used Oil": {
        "name": "Oil Recycling",
        "email": "oilrecycling@supplier.com"
    },
    "Referral Distribution Program": {
        "name": "Distribution Partner",
        "email": "distribution@partner.com"
    },
    "Solar Energy PPA": {
        "name": "Solar Energy",
        "email": "data.quote@fornrg.com"
    },
    "Self Managed Certificates": {
        "name": "Certificate Management",
        "email": "certificates@supplier.com"
    },
    "Telecommunication": {
        "name": "Telecom",
        "email": "telecom@supplier.com"
    },
    "Wood Pallet": {
        "name": "Wood Pallet",
        "email": "woodpallet@supplier.com"
    },
    "Wood Cut": {
        "name": "Wood Processing",
        "email": "woodprocessing@supplier.com"
    },
    "Baled Cardboard": {
        "name": "Cardboard Recycling",
        "email": "cardboard@recycler.com"
    },
    "Loose Cardboard": {
        "name": "Cardboard Recycling",
        "email": "cardboard@recycler.com"
    },
    "Large Generation Certificates Trading": {
        "name": "LGC Trading",
        "email": "lgc@trading.com"
    },
    "GHG Action Plan": {
        "name": "Environmental",
        "email": "environment@supplier.com"
    },
    "Government Incentives Vic G4": {
        "name": "Government",
        "email": "government@supplier.com"
    },
    "Self Managed VEECs": {
        "name": "VEEC Management",
        "email": "veec@management.com"
    },
    "Demand Response": {
        "name": "Demand Response",
        "email": "demandresponse@supplier.com"
    },
    "Waste Organic Recycling": {
        "name": "Organic Waste",
        "email": "organicwaste@recycler.com"
    },
    "Waste Grease Trap": {
        "name": "Grease Trap",
        "email": "greasetrap@supplier.com"
    },
    "Used Wax Cardboard": {
        "name": "Wax Cardboard",
        "email": "waxcardboard@recycler.com"
    },
    "Vic CDS Scheme": {
        "name": "CDS Scheme",
        "email": "cds@scheme.com"
    },
    "New Placeholder Template": {
        "name": "Template",
        "email": "members@acesolutions.com.au, data.quote@fornrg.com"
    }
}

# Default fallback email
DEFAULT_EMAIL = {
    "name": "Unknown Supplier",
    "email": "members@acesolutions.com.au"
}

def find_supplier_email_for_agreement(contract_type: str, agreement_type: str) -> Tuple[str, str, bool]:
    """
    Find the supplier email address based on contract type and agreement type
    
    Args:
        contract_type: Type of contract/EOI
        agreement_type: Type of agreement - either "contract" or "eoi"
        
    Returns:
        Tuple of (email_address, resolved_name, is_default)
    """
    logger.info(f"Looking up email for contract type: '{contract_type}', agreement type: '{agreement_type}'")

    flow = "eoi" if agreement_type == "eoi" else "signed_contract"
    try:
        from services.operational_emails import emails_csv, find_recipient_match, with_db

        match = with_db(lambda db: find_recipient_match(db, flow, contract_type))
        if match:
            logger.info(f"DB match found: {match.display_name}")
            return emails_csv(match), match.display_name, False
    except Exception as e:
        logger.warning("signed agreement recipient DB lookup failed: %s", e)
    
    # Select the appropriate mapping based on agreement type
    if agreement_type == "eoi":
        mapping = EOI_EMAIL_MAPPINGS
    else:
        mapping = CONTRACT_EMAIL_MAPPINGS
    
    # Try exact match first
    if contract_type in mapping:
        supplier_info = mapping[contract_type]
        logger.info(f"Exact match found: {supplier_info['name']}")
        return supplier_info["email"], supplier_info["name"], False
    
    # Try case-insensitive match
    for key, value in mapping.items():
        if key.lower() == contract_type.lower():
            logger.info(f"Case-insensitive match found: {value['name']}")
            return value["email"], value["name"], False
    
    # Use default email if no match found
    logger.warning(f"No match found for contract type: '{contract_type}', using default email")
    return DEFAULT_EMAIL["email"], DEFAULT_EMAIL["name"], True


def recipient_emails_csv(value: str | None) -> str | None:
    """Normalise a one-send recipient override. Blank means 'not provided'."""
    if value is None:
        return None
    seen: set[str] = set()
    cleaned: list[str] = []
    for part in str(value).replace(";", ",").split(","):
        email = part.strip()
        if not email:
            continue
        key = email.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(email)
    if not cleaned:
        return None
    return ", ".join(cleaned)


def resolve_lodgement_recipients(
    contract_type: str,
    agreement_type: str,
    recipient_emails: str | None = None,
) -> Tuple[str, str, bool]:
    supplier_email, resolved_name, is_default = find_supplier_email_for_agreement(
        contract_type, agreement_type
    )
    override = recipient_emails_csv(recipient_emails)
    if override:
        return override, resolved_name, False
    return supplier_email, resolved_name, is_default


def _signed_agreement_email_fields(
    business_name: str,
    contract_type: str,
    agreement_type: str,
    identifier: str | None = None,
    identifier_type: str | None = None,
) -> dict[str, str]:
    if agreement_type == "eoi":
        agreement_label = "EOI"
    elif agreement_type == "contract_multiple_attachments":
        agreement_label = "contracts"
    else:
        agreement_label = "contract"
    identifier_html = ""
    if identifier and identifier_type:
        identifier_html = f"<p>{identifier_type.upper()}: {identifier}</p>"
    values = {
        "business_name": business_name,
        "contract_type": contract_type,
        "agreement_label": agreement_label,
        "identifier_html": identifier_html,
        "nmi": identifier if identifier_type == "nmi" else "",
        "mirn": identifier if identifier_type == "mirn" else "",
    }
    try:
        from services.operational_emails import load_template_content, render_tokens, with_db

        loaded = with_db(lambda db: load_template_content(db, "signed_agreement.default"))
        if loaded:
            return {
                "email_subject": render_tokens(loaded[0], values),
                "email_html_content": render_tokens(loaded[1], values),
            }
    except Exception as e:
        logger.warning("signed agreement template lookup failed: %s", e)
    subject = f"Signed {agreement_label} - {business_name} - {contract_type}"
    html = (
        f"<p>Hello Team,</p><p>Please find attached the signed {agreement_label} "
        f"for our member <strong>{business_name}</strong>.</p>"
        f"<p>Document type: {contract_type}</p>{identifier_html}"
    )
    return {"email_subject": subject, "email_html_content": html}


UTILITY_FILING_TYPES = {
    "C&I Electricity": "signed_CI_E",
    "SME Electricity": "signed_SME_E",
    "C&I Gas": "signed_CI_G",
    "SME Gas": "signed_SME_G",
    "Waste": "signed_WASTE",
    "Oil": "signed_OIL",
    "DMA": "signed_DMA",
}

_FILING_LABELS_LONGEST_FIRST = sorted(UTILITY_FILING_TYPES, key=len, reverse=True)

NOT_MAPPED_DRIVE_NOTE = "\n\n(No drive filing performed: contract type not mapped)"


def filing_type_for_supplier(contract_type: str, utility_type: str | None = None) -> str | None:
    """Map a supplier option or utility label to a Drive filing type.

    "Alinta C&I Electricity" and utility_type "C&I Electricity" both resolve to signed_CI_E.
    Longer utility labels are tried first so "SME Electricity" is not confused with a shorter token.
    """
    explicit = (utility_type or "").strip()
    if explicit in UTILITY_FILING_TYPES:
        return UTILITY_FILING_TYPES[explicit]
    name = (contract_type or "").strip()
    if name in UTILITY_FILING_TYPES:
        return UTILITY_FILING_TYPES[name]
    lowered = name.lower()
    for label in _FILING_LABELS_LONGEST_FIRST:
        token = label.lower()
        if lowered == token or lowered.endswith(token) or f" {token}" in f" {lowered}":
            return UTILITY_FILING_TYPES[label]
    return None


def utility_label_for_lodgement(contract_type: str, utility_type: str | None = None) -> str:
    explicit = (utility_type or "").strip()
    if explicit:
        return explicit
    name = (contract_type or "").strip()
    lowered = name.lower()
    for label in _FILING_LABELS_LONGEST_FIRST:
        token = label.lower()
        if lowered == token or lowered.endswith(token) or f" {token}" in f" {lowered}":
            return label
    return name


def unmapped_drive_note(
    contract_type: str,
    utility_type: str | None = None,
    skip_drive_filing: bool = False,
) -> str:
    if skip_drive_filing:
        return ""
    if filing_type_for_supplier(contract_type, utility_type):
        return ""
    return NOT_MAPPED_DRIVE_NOTE


def resolve_lodgement_identifier(
    business_name: str,
    nmi: str | None = None,
    mirn: str | None = None,
) -> tuple[str, str | None, str | None]:
    """Return (business name without NMI/MIRN suffix, identifier, identifier type).

    An explicit nmi or mirn argument wins over a suffix parsed from the business name.
    """
    explicit_nmi = (nmi or "").strip()
    explicit_mirn = (mirn or "").strip()
    nmi_match = re.search(r"NMI:\s*(\d+)", business_name or "")
    mirn_match = re.search(r"MIRN:\s*(\d+)", business_name or "")
    if explicit_nmi:
        identifier, identifier_type = explicit_nmi, "nmi"
    elif explicit_mirn:
        identifier, identifier_type = explicit_mirn, "mirn"
    elif nmi_match:
        identifier, identifier_type = nmi_match.group(1), "nmi"
    elif mirn_match:
        identifier, identifier_type = mirn_match.group(1), "mirn"
    else:
        identifier, identifier_type = None, None
    actual = re.sub(r"\s*(?:NMI|MIRN):\s*\S+", "", business_name or "").strip()
    return actual, identifier, identifier_type


def send_supplier_signed_agreement(
    file_path: str,
    business_name: str,
    contract_type: str,
    agreement_type: str = "contract",
    nmi: str | None = None,
    mirn: str | None = None,
    utility_type: str | None = None,
    skip_drive_filing: bool = False,
    recipient_emails: str | None = None,
) -> str:
    """
    Send a signed supplier agreement (Contract or EOI) to a supplier via email.
    
    Args:
        file_path: Path to the signed agreement file to be sent
        business_name: Name of the business (optionally with identifier: "Business Name NMI: 12345" or "Business Name MIRN: 12345")
        contract_type: Type of contract/EOI (e.g., PowerMetric DMA, Direct Meter Agreement)
        agreement_type: Type of agreement - either "contract" or "eoi" (default: "contract")
    
    Returns:
        String with success/error message and details
    """
    logger.info(f"Processing signed agreement: {contract_type} ({agreement_type})")

    actual_business_name, identifier, identifier_type = resolve_lodgement_identifier(
        business_name, nmi, mirn
    )

    supplier_email, resolved_supplier_name, is_default = resolve_lodgement_recipients(
        contract_type, agreement_type, recipient_emails
    )
    logger.info(f"Resolved supplier email: {supplier_email} for {resolved_supplier_name} (default: {is_default})")
    
    # Prepare file and payload
    try:
        files = {
            "file": (
                os.path.basename(file_path),  # Get filename from path
                open(file_path, "rb"),
                "application/octet-stream",
            )
        }
    except Exception as e:
        logger.error(f"Error opening file: {str(e)}")
        return f"❌ Error: Could not open file at {file_path}"

    payload = {
        "business_name": actual_business_name,
        "contract_type": contract_type,
        "agreement_type": agreement_type,
        "supplier_email": supplier_email,
        "resolved_supplier_name": resolved_supplier_name,
    }
    payload.update(
        _signed_agreement_email_fields(
            actual_business_name,
            contract_type,
            agreement_type,
            identifier,
            identifier_type,
        )
    )
    
    # Add identifier if present
    if identifier and identifier_type:
        payload[identifier_type] = identifier

    # Send the request
    try:
        response = requests.post(
            "https://membersaces.app.n8n.cloud/webhook/email-supplier",
            data=payload,
            files=files,
        )
        
        if response.status_code == 200:
            # Build success message
            document_name = "EOI" if agreement_type == "eoi" else "contract"
            
            # Heading
            success_msg = f"""✅ **The signed {document_name} has been successfully sent:**\n\n"""
            # Document details
            success_msg += f"📄 **Document:** {contract_type}\n"
            success_msg += f"🏢 **Business:** {actual_business_name}\n"
            success_msg += f"✉️ **Sent to:** {supplier_email}\n"
            success_msg += f"🏷️ **Supplier:** {resolved_supplier_name}"
            # Identifier if present
            if identifier and identifier_type:
                success_msg += f"\n🔢 **{identifier_type.upper()}:** {identifier}"
            # Warning if default
            if is_default:
                success_msg += f"\n\n⚠️ **Note:** '{contract_type}' was not recognized. The {document_name} will be sent to our general members email ({supplier_email}) for manual processing."
            
            # Log the success
            logger.info(f"Successfully sent {document_name} to {resolved_supplier_name}")
            
            # Parse response if it contains additional info
            try:
                response_data = response.json()
                if "message" in response_data:
                    logger.info(f"API Response: {response_data['message']}")
            except:
                pass
            
            # Second Drive upload for the standalone lodgement page.
            # The Documents tab files first and sends skip_drive_filing=true.
            # Do not send contract_status: that cell is owned by the File in Drive step.
            if not skip_drive_filing:
                note = unmapped_drive_note(contract_type, utility_type, skip_drive_filing=False)
                if note:
                    success_msg += note
                elif drive_filing and get_business_information:
                    filing_type = filing_type_for_supplier(contract_type, utility_type)
                    try:
                        business_info = get_business_information(actual_business_name) or {}
                        gdrive = business_info.get("gdrive") if isinstance(business_info, dict) else None
                        gdrive_url = ""
                        if isinstance(gdrive, dict):
                            gdrive_url = str(gdrive.get("folder_url") or "").strip()
                        if filing_type and gdrive_url:
                            with open(file_path, "rb") as filed:
                                payload_bytes = filed.read()
                            drive_filing_result = drive_filing(
                                file_payloads=[(payload_bytes, os.path.basename(file_path))],
                                business_name=actual_business_name,
                                gdrive_url=gdrive_url,
                                filing_type=filing_type,
                                contract_update_mode="append",
                            )
                            success_msg += f"\n\n**Drive Filing Result:** {drive_filing_result}"
                        elif filing_type:
                            success_msg += "\n\n(No drive filing performed: no Google Drive folder)"
                    except Exception as e:
                        logger.error(f"Drive filing error: {str(e)}")
                        success_msg += f"\n\n**Drive Filing Error:** {str(e)}"
                else:
                    success_msg += "\n\n(Drive filing not available)"
            
            return success_msg
        else:
            logger.error(f"Failed to send agreement. Status code: {response.status_code}")
            return f"❌ Error: Failed to send signed {agreement_type} to supplier. Status code: {response.status_code}"
            
    except Exception as e:
        logger.error(f"Error sending request: {str(e)}")
        return f"❌ Error: Failed to send signed {agreement_type} - {str(e)}"
    finally:
        # Close the file
        if 'files' in locals() and 'file' in files:
            try:
                files['file'][1].close()
            except:
                pass

def send_supplier_signed_agreement_multiple(
    file_paths: list,
    business_name: str,
    contract_type: str,
    agreement_type: str = "contract_multiple_attachments",
    filenames: list = None,
    nmi: str | None = None,
    mirn: str | None = None,
    utility_type: str | None = None,
    skip_drive_filing: bool = False,
    recipient_emails: str | None = None,
) -> str:
    """
    Send multiple signed supplier agreements to a supplier via email.
    
    Args:
        file_paths: List of paths to the signed agreement files to be sent
        business_name: Name of the business (optionally with identifier: "Business Name NMI: 12345" or "Business Name MIRN: 12345")
        contract_type: Type of contract (e.g., PowerMetric DMA)
        agreement_type: Type of agreement - should be "contract_multiple_attachments"
        filenames: List of original filenames (optional)
    
    Returns:
        String with success/error message and details
    """
    logger.info(
        f"Processing multiple signed agreements: {contract_type} ({agreement_type}) "
        f"utility={utility_type} skip_drive_filing={skip_drive_filing} - {len(file_paths)} files"
    )

    actual_business_name, identifier, identifier_type = resolve_lodgement_identifier(
        business_name, nmi, mirn
    )

    supplier_email, resolved_supplier_name, is_default = resolve_lodgement_recipients(
        contract_type, "contract", recipient_emails
    )
    logger.info(f"Resolved supplier email: {supplier_email} for {resolved_supplier_name} (default: {is_default})")
    
    # Prepare files and payload
    files = {}
    try:
        for idx, file_path in enumerate(file_paths):
            filename = filenames[idx] if filenames and idx < len(filenames) else os.path.basename(file_path)
            files[f"file_{idx}"] = (
                filename,
                open(file_path, "rb"),
                "application/octet-stream",
            )
    except Exception as e:
        logger.error(f"Error opening files: {str(e)}")
        return f"❌ Error: Could not open files: {str(e)}"

    payload = {
        "business_name": actual_business_name,
        "contract_type": contract_type,
        "agreement_type": agreement_type,
        "supplier_email": supplier_email,
        "resolved_supplier_name": resolved_supplier_name,
        "file_count": len(file_paths),
    }
    payload.update(
        _signed_agreement_email_fields(
            actual_business_name,
            contract_type,
            agreement_type,
            identifier,
            identifier_type,
        )
    )
    
    # Add identifier if present
    if identifier and identifier_type:
        payload[identifier_type] = identifier

    # Send the request
    try:
        response = requests.post(
            "https://membersaces.app.n8n.cloud/webhook/email-supplier",
            data=payload,
            files=files,
        )
        
        if response.status_code == 200:
            # Build success message
            success_msg = f"""✅ **{len(file_paths)} signed contracts have been successfully sent:**\n\n"""
            # Document details
            success_msg += f"📄 **Contract Type:** {contract_type}\n"
            success_msg += f"🏢 **Business:** {actual_business_name}\n"
            success_msg += f"✉️ **Sent to:** {supplier_email}\n"
            success_msg += f"🏷️ **Supplier:** {resolved_supplier_name}\n"
            success_msg += f"📎 **Files:** {len(file_paths)} attachments"
            
            # Show filenames if available
            if filenames:
                success_msg += f"\n📋 **Filenames:** {', '.join(filenames)}"
            
            # Identifier if present
            if identifier and identifier_type:
                success_msg += f"\n🔢 **{identifier_type.upper()}:** {identifier}"
            
            # Warning if default
            if is_default:
                success_msg += f"\n\n⚠️ **Note:** '{contract_type}' was not recognized. The contracts will be sent to our general members email ({supplier_email}) for manual processing."
            
            # Log the success
            logger.info(f"Successfully sent {len(file_paths)} contracts to {resolved_supplier_name}")
            
            # Parse response if it contains additional info
            try:
                response_data = response.json()
                if "message" in response_data:
                    logger.info(f"API Response: {response_data['message']}")
            except:
                pass
            
            # Note about drive filing for multiple attachments
            success_msg += "\n\n📁 **Note:** Drive filing for multiple attachments will be handled by the automation workflow."
            
            return success_msg
        else:
            logger.error(f"Failed to send multiple agreements. Status code: {response.status_code}")
            return f"❌ Error: Failed to send {len(file_paths)} signed contracts to supplier. Status code: {response.status_code}"
            
    except Exception as e:
        logger.error(f"Error sending multiple agreements request: {str(e)}")
        return f"❌ Error: Failed to send {len(file_paths)} signed contracts - {str(e)}"
    finally:
        # Close all the files
        for file_key, file_tuple in files.items():
            try:
                file_tuple[1].close()
            except:
                pass
            
# Utility function to get available contract types for API
def get_available_contract_types() -> Dict[str, list]:
    """
    Get all available contract types organized by type
    
    Returns:
        Dict with 'contracts' and 'eois' keys containing lists of available types
    """
    return {
        "contracts": list(CONTRACT_EMAIL_MAPPINGS.keys()),
        "eois": list(EOI_EMAIL_MAPPINGS.keys())
    }