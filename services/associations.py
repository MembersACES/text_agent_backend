"""Association records and the Drive folder created for each one."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from models import Association, Testimonial
from tools.association_folders import (
    ASSOCIATION_SOLUTION_TYPE_ID,
    ASSOCIATION_SOLUTION_TYPE_LABEL,
    NO_INVOICE_RECORDED,
    clean_association_name,
    create_named_folder,
    ensure_testimonials_folder,
    get_associations_parent_id,
    list_association_documents,
    list_association_drive_folders,
    normalize_association_status,
    rename_drive_folder,
    upload_association_document,
)
from tools.image_to_pdf import image_bytes_to_pdf, is_image_filename
from tools.member_folder_drive import MemberFolderDriveError
from tools.share_folder import drive_folder_url
from tools.testimonial_solution_content import build_testimonial_file_name

logger = logging.getLogger(__name__)

_TESTIMONIAL_EXTENSIONS = (".pdf", ".docx", ".doc", ".png", ".jpg", ".jpeg")


def _blank(value: Optional[str]) -> Optional[str]:
    text = (value or "").strip()
    return text or None


def _counts(db: Session) -> Dict[int, Dict[str, int]]:
    rows = (
        db.query(Testimonial)
        .filter(Testimonial.association_id.isnot(None))
        .all()
    )
    buckets: Dict[int, Dict[str, int]] = {}
    for row in rows:
        bucket = buckets.setdefault(int(row.association_id), {"total": 0, "approved": 0})
        bucket["total"] += 1
        if (row.status or "") == "Approved":
            bucket["approved"] += 1
    return buckets


def association_payload(
    row: Association,
    counts: Dict[int, Dict[str, int]],
    warnings: Optional[List[str]] = None,
) -> Dict[str, Any]:
    bucket = counts.get(int(row.id), {"total": 0, "approved": 0})
    approved = int(bucket["approved"])
    return {
        "id": row.id,
        "name": row.name,
        "status": row.status,
        "endorsed": approved > 0,
        "drive_folder_id": row.drive_folder_id,
        "drive_folder_url": row.drive_folder_url,
        "testimonials_folder_id": row.testimonials_folder_id,
        "contact_name": row.contact_name,
        "contact_email": row.contact_email,
        "notes": row.notes,
        "results_note": row.results_note,
        "testimonial_count": int(bucket["total"]),
        "approved_testimonial_count": approved,
        "warnings": list(warnings or []),
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _parent_payload() -> Dict[str, str]:
    parent_id = get_associations_parent_id()
    return {
        "parent_folder_id": parent_id,
        "parent_folder_url": drive_folder_url(parent_id),
    }


def _name_taken(db: Session, name: str, exclude_id: Optional[int] = None) -> bool:
    query = db.query(Association).filter(func.lower(Association.name) == name.lower())
    if exclude_id is not None:
        query = query.filter(Association.id != exclude_id)
    return query.first() is not None


def list_associations(db: Session) -> Dict[str, Any]:
    rows = db.query(Association).order_by(func.lower(Association.name)).all()
    counts = _counts(db)
    payload = _parent_payload()
    payload["associations"] = [association_payload(row, counts) for row in rows]
    return payload


def _get(db: Session, association_id: int) -> Optional[Association]:
    return db.query(Association).filter(Association.id == association_id).first()


def create_association(
    db: Session,
    *,
    name: str,
    status: str,
    contact_name: Optional[str],
    contact_email: Optional[str],
    notes: Optional[str],
    results_note: Optional[str],
    user_access_token: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    cleaned = clean_association_name(name)
    if not cleaned:
        return None, "Association name is required.", 400
    normalized = normalize_association_status(status or "targeting")
    if not normalized:
        return None, "Status must be Targeting or Working with.", 400
    if _name_taken(db, cleaned):
        return None, "An association with that name already exists.", 409

    parent_id = get_associations_parent_id()
    warnings: List[str] = []
    try:
        folder_id, _created = create_named_folder(parent_id, cleaned, user_access_token)
    except MemberFolderDriveError as exc:
        return None, exc.message, exc.status_code

    linked = (
        db.query(Association).filter(Association.drive_folder_id == folder_id).first()
    )
    if linked:
        return None, f"That Drive folder is already linked to {linked.name}.", 409

    testimonials_folder_id = None
    try:
        testimonials_folder_id = ensure_testimonials_folder(folder_id, user_access_token)
    except MemberFolderDriveError as exc:
        warnings.append(f"Could not create the Testimonials folder: {exc.message}")

    row = Association(
        name=cleaned,
        status=normalized,
        drive_folder_id=folder_id,
        drive_folder_url=drive_folder_url(folder_id),
        testimonials_folder_id=testimonials_folder_id,
        contact_name=_blank(contact_name),
        contact_email=_blank(contact_email),
        notes=_blank(notes),
        results_note=_blank(results_note),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return association_payload(row, _counts(db), warnings), None, 200


def update_association(
    db: Session,
    association_id: int,
    fields: Dict[str, Any],
    user_access_token: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    row = _get(db, association_id)
    if not row:
        return None, "Association not found.", 404

    if "name" in fields and fields["name"] is not None:
        cleaned = clean_association_name(str(fields["name"]))
        if not cleaned:
            return None, "Association name is required.", 400
        if cleaned.lower() != (row.name or "").lower() and _name_taken(db, cleaned, exclude_id=row.id):
            return None, "An association with that name already exists.", 409
        if cleaned != row.name and row.drive_folder_id:
            try:
                rename_drive_folder(row.drive_folder_id, cleaned, user_access_token)
            except MemberFolderDriveError as exc:
                return None, exc.message, exc.status_code
        row.name = cleaned

    if "status" in fields and fields["status"] is not None:
        normalized = normalize_association_status(str(fields["status"]))
        if not normalized:
            return None, "Status must be Targeting or Working with.", 400
        row.status = normalized

    for key in ("contact_name", "contact_email", "notes", "results_note"):
        if key in fields:
            setattr(row, key, _blank(None if fields[key] is None else str(fields[key])))

    db.commit()
    db.refresh(row)
    return association_payload(row, _counts(db)), None, 200


def sync_associations_from_drive(
    db: Session,
    user_access_token: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    payload, err, status = list_association_drive_folders(user_access_token)
    if err or not payload:
        return None, err, status

    existing = db.query(Association).all()
    by_folder = {row.drive_folder_id: row for row in existing if row.drive_folder_id}
    by_name = {(row.name or "").strip().lower(): row for row in existing}
    adopted = 0
    linked = 0
    skipped: List[str] = []

    for folder in payload.get("folders") or []:
        folder_id = str(folder.get("id") or "").strip()
        name = clean_association_name(str(folder.get("name") or ""))
        if not folder_id or not name:
            continue
        if folder_id in by_folder:
            continue
        named = by_name.get(name.lower())
        if named and named.drive_folder_id and named.drive_folder_id != folder_id:
            skipped.append(f"{name} is already linked to a different folder.")
            continue
        testimonials_folder_id = None
        try:
            testimonials_folder_id = ensure_testimonials_folder(folder_id, user_access_token)
        except MemberFolderDriveError as exc:
            logger.warning("Testimonials folder for %s: %s", name, exc.message)
        if named and not named.drive_folder_id:
            named.drive_folder_id = folder_id
            named.drive_folder_url = folder.get("folder_url") or drive_folder_url(folder_id)
            if testimonials_folder_id:
                named.testimonials_folder_id = testimonials_folder_id
            by_folder[folder_id] = named
            linked += 1
            continue
        row = Association(
            name=name,
            status="targeting",
            drive_folder_id=folder_id,
            drive_folder_url=folder.get("folder_url") or drive_folder_url(folder_id),
            testimonials_folder_id=testimonials_folder_id,
        )
        db.add(row)
        db.flush()
        by_folder[folder_id] = row
        by_name[name.lower()] = row
        adopted += 1

    db.commit()
    listed = list_associations(db)
    return {
        "adopted": adopted,
        "linked": linked,
        "skipped": skipped,
        **listed,
    }, None, 200


def list_association_files(
    db: Session,
    association_id: int,
    folder_id: Optional[str],
    user_access_token: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    row = _get(db, association_id)
    if not row:
        return None, "Association not found.", 404
    if not row.drive_folder_id:
        return None, "This association does not have a Drive folder yet.", 404
    target = (folder_id or "").strip() or row.drive_folder_id
    payload, err, status = list_association_documents(
        row.drive_folder_id,
        target,
        user_access_token,
    )
    if err or not payload:
        if err == "folder_not_under_association":
            return None, "That folder is not inside this association.", 404
        if err == "missing_folder_id":
            return None, "A folder id is required.", 400
        return None, err, status
    payload["association_id"] = row.id
    return payload, None, 200


def list_association_testimonials(db: Session, association_id: int) -> Tuple[Optional[List[Testimonial]], Optional[str], int]:
    row = _get(db, association_id)
    if not row:
        return None, "Association not found.", 404
    items = (
        db.query(Testimonial)
        .filter(Testimonial.association_id == row.id)
        .order_by(Testimonial.created_at.desc())
        .all()
    )
    return items, None, 200


def _ensure_testimonials_id(
    db: Session,
    row: Association,
    user_access_token: Optional[str],
) -> Tuple[Optional[str], Optional[str], int]:
    if row.testimonials_folder_id:
        return row.testimonials_folder_id, None, 200
    if not row.drive_folder_id:
        return None, "This association does not have a Drive folder yet.", 404
    try:
        folder_id = ensure_testimonials_folder(row.drive_folder_id, user_access_token)
    except MemberFolderDriveError as exc:
        return None, exc.message, exc.status_code
    row.testimonials_folder_id = folder_id
    db.commit()
    db.refresh(row)
    return folder_id, None, 200


def upload_association_file(
    db: Session,
    association_id: int,
    *,
    folder_id: Optional[str],
    file_bytes: bytes,
    filename: str,
    content_type: Optional[str],
    display_name: Optional[str],
    register_testimonial: bool,
    testimonial_savings: Optional[str],
    user_access_token: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    row = _get(db, association_id)
    if not row:
        return None, "Association not found.", 404
    if not row.drive_folder_id:
        return None, "This association does not have a Drive folder yet.", 404

    chosen_name = (display_name or filename or "upload").strip()
    dest = (folder_id or "").strip() or row.drive_folder_id
    testimonial_row = None

    if register_testimonial:
        lower = (filename or chosen_name).lower()
        if not any(lower.endswith(ext) for ext in _TESTIMONIAL_EXTENSIONS):
            return None, "A testimonial must be a PDF, Word document, or image.", 400
        if is_image_filename(filename or chosen_name):
            try:
                file_bytes, filename = image_bytes_to_pdf(file_bytes, filename or chosen_name)
                content_type = "application/pdf"
            except ValueError as exc:
                return None, str(exc), 400
            if not chosen_name.lower().endswith(".pdf"):
                stem = chosen_name.rsplit(".", 1)[0] if "." in chosen_name else chosen_name
                chosen_name = f"{stem}.pdf"
        dest_id, err, status = _ensure_testimonials_id(db, row, user_access_token)
        if err or not dest_id:
            return None, err, status
        dest = dest_id
        chosen_name = build_testimonial_file_name(
            ASSOCIATION_SOLUTION_TYPE_LABEL,
            row.name,
            original_upload_basename=chosen_name,
        )

    uploaded, err, status = upload_association_document(
        row.drive_folder_id,
        dest,
        file_bytes,
        filename or chosen_name,
        content_type=content_type,
        display_name=chosen_name,
        user_access_token=user_access_token,
    )
    if err or not uploaded:
        if err == "empty_file":
            return None, "The file is empty.", 400
        if err == "file_too_large":
            return None, "File is larger than 50 MB.", 400
        if err == "folder_not_under_association":
            return None, "That folder is not inside this association.", 404
        if err == "missing_folder_id":
            return None, "A folder id is required.", 400
        return None, err, status

    if register_testimonial:
        savings = _blank(testimonial_savings)
        if savings and len(savings) > 255:
            savings = savings[:255]
        testimonial_row = Testimonial(
            business_name=row.name,
            file_name=uploaded["name"],
            file_id=uploaded["id"],
            invoice_number=NO_INVOICE_RECORDED,
            status="Draft",
            testimonial_type=ASSOCIATION_SOLUTION_TYPE_LABEL,
            testimonial_solution_type_id=ASSOCIATION_SOLUTION_TYPE_ID,
            testimonial_savings=savings,
            association_id=row.id,
        )
        db.add(testimonial_row)
        db.commit()
        db.refresh(testimonial_row)

    return {
        **uploaded,
        "association_id": row.id,
        "testimonial": testimonial_row,
    }, None, 200
