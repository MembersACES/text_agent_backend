"""Commercial entity group summary and suggestion helpers."""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Literal, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from models import Client, ClimateActivityRecord, EntityGroup, Offer

Confidence = Literal["high", "medium", "low"]

_LEGAL_SUFFIX_PATTERNS = [
    r"\bpty\s+ltd\.?\b",
    r"\bpty\s+limited\b",
    r"\bproprietary\s+limited\b",
    r"\blimited\b",
    r"\bltd\.?\b",
    r"\btrust\b",
    r"\bas\s+trustee\s+for\b",
    r"\batf\b",
    r"\binc\.?\b",
    r"\bincorporated\b",
]

_COMMON_EMAIL_DOMAINS = frozenset(
    {
        "gmail.com",
        "googlemail.com",
        "hotmail.com",
        "outlook.com",
        "yahoo.com",
        "icloud.com",
        "live.com",
        "bigpond.com",
        "optusnet.com.au",
    }
)


def slugify_display_name(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s


def normalize_business_name(name: str) -> str:
    s = (name or "").strip().lower()
    for pattern in _LEGAL_SUFFIX_PATTERNS:
        s = re.sub(pattern, " ", s, flags=re.IGNORECASE)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _name_tokens(name: str) -> set[str]:
    normalized = normalize_business_name(name)
    if not normalized:
        return set()
    return {t for t in normalized.split() if len(t) >= 2}


def _email_domain(email: Optional[str]) -> Optional[str]:
    if not email or "@" not in email:
        return None
    domain = email.strip().split("@", 1)[1].lower().strip()
    if not domain or domain in _COMMON_EMAIL_DOMAINS:
        return None
    return domain


def _names_match(a: str, b: str) -> tuple[bool, Confidence]:
    na = normalize_business_name(a)
    nb = normalize_business_name(b)
    if not na or not nb:
        return False, "low"
    if na == nb:
        return True, "high"
    if na.startswith(nb) or nb.startswith(na):
        return True, "high"
    ta, tb = _name_tokens(a), _name_tokens(b)
    if not ta or not tb:
        return False, "low"
    overlap = ta & tb
    if not overlap:
        return False, "low"
    min_len = min(len(ta), len(tb))
    ratio = len(overlap) / min_len
    if ratio >= 0.6 or len(overlap) >= 2:
        return True, "medium"
    if len(overlap) >= 1 and (na in nb or nb in na):
        return True, "medium"
    return False, "low"


def _pair_reason(a: Client, b: Client) -> str:
    na = normalize_business_name(a.business_name)
    nb = normalize_business_name(b.business_name)
    if na == nb:
        return f'Same normalized name: "{na}"'
    if na.startswith(nb) or nb.startswith(na):
        return f'Similar business names: "{a.business_name}" and "{b.business_name}"'
    da, db = _email_domain(a.primary_contact_email), _email_domain(b.primary_contact_email)
    if da and db and da == db:
        return f"Shared contact domain @{da} with similar names"
    return f'Similar business names: "{a.business_name}" and "{b.business_name}"'


class _UnionFind:
    def __init__(self, n: int) -> None:
        self.parent = list(range(n))

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def _cluster_confidence(members: list[Client], pair_confidences: list[Confidence]) -> Confidence:
    if "high" in pair_confidences:
        return "high"
    domains = [_email_domain(m.primary_contact_email) for m in members]
    non_null = [d for d in domains if d]
    if len(non_null) >= 2 and len(set(non_null)) == 1 and "medium" in pair_confidences:
        return "medium"
    if "medium" in pair_confidences:
        return "medium"
    return "low"


def effective_reporting_entity(
    client: Client,
    group: EntityGroup | None = None,
) -> str | None:
    """Site override wins; else inherit group disclosure slug."""
    if client.reporting_entity and str(client.reporting_entity).strip():
        return str(client.reporting_entity).strip().lower()
    if group and group.reporting_entity and str(group.reporting_entity).strip():
        return str(group.reporting_entity).strip().lower()
    if client.entity_group_id and group is None:
        return None
    return None


def clients_in_disclosure_rollup(db: Session, slug: str) -> list[Client]:
    """
    CRM clients whose LOA/staged activity roll up to a disclosure slug.
    Includes direct member slug match and group-inheriting members (null member slug).
    """
    normalized = (slug or "").strip().lower()
    if not normalized:
        return []

    direct = (
        db.query(Client)
        .filter(Client.reporting_entity == normalized)
        .order_by(Client.id.asc())
        .all()
    )
    direct_ids = {c.id for c in direct}

    inherit = (
        db.query(Client)
        .join(EntityGroup, Client.entity_group_id == EntityGroup.id)
        .filter(Client.reporting_entity.is_(None))
        .filter(EntityGroup.reporting_entity == normalized)
        .order_by(Client.id.asc())
        .all()
    )

    out = list(direct)
    for client in inherit:
        if client.id not in direct_ids:
            out.append(client)
    return out


def disclosure_source_for_client(client: Client, slug: str, group: EntityGroup | None) -> str:
    normalized = (slug or "").strip().lower()
    member_slug = (client.reporting_entity or "").strip().lower()
    if member_slug == normalized:
        return "member"
    if not member_slug and group and (group.reporting_entity or "").strip().lower() == normalized:
        return "group_inherit"
    return "member"


def build_entity_group_summary(db: Session, group: EntityGroup) -> dict:
    members = (
        db.query(Client)
        .filter(Client.entity_group_id == group.id)
        .all()
    )
    member_count = len(members)
    member_ids = [m.id for m in members]

    total_offers = 0
    if member_ids:
        total_offers = (
            db.query(func.count(Offer.id))
            .filter(Offer.client_id.in_(member_ids), Offer.client_id.isnot(None))
            .scalar()
            or 0
        )

    any_signed = any(m.has_signed_contract == 1 for m in members)

    stage_breakdown: dict[str, int] = {}
    for m in members:
        stage_breakdown[m.stage] = stage_breakdown.get(m.stage, 0) + 1

    distinct_values = sorted(
        {m.reporting_entity for m in members if m.reporting_entity}
    )
    aligned = len(distinct_values) <= 1

    group_slug = (group.reporting_entity or "").strip().lower() or None
    rollup_members: list[Client] = []
    if group_slug:
        rollup_members = clients_in_disclosure_rollup(db, group_slug)
        rollup_members = [m for m in rollup_members if m.entity_group_id == group.id]
    elif distinct_values:
        for val in distinct_values:
            rollup_members.extend(
                m for m in members if (m.reporting_entity or "").strip().lower() == val
            )

    rollup_ids = {m.id for m in rollup_members}
    staged_activity_total = 0
    if rollup_ids:
        staged_activity_total = (
            db.query(func.count(ClimateActivityRecord.id))
            .filter(ClimateActivityRecord.client_id.in_(list(rollup_ids)))
            .scalar()
            or 0
        )

    return {
        "member_count": member_count,
        "total_offers": int(total_offers),
        "any_signed": any_signed,
        "stage_breakdown": stage_breakdown,
        "group_reporting_entity": group_slug,
        "members_in_climate_rollup": len(rollup_members),
        "staged_activity_total": int(staged_activity_total),
        "reporting_entity": {
            "aligned": aligned,
            "distinct_values": distinct_values,
        },
    }


def compute_entity_group_suggestions(db: Session) -> list[dict]:
    ungrouped = (
        db.query(Client)
        .filter(Client.entity_group_id.is_(None))
        .order_by(Client.business_name.asc())
        .all()
    )
    n = len(ungrouped)
    if n < 2:
        return []

    uf = _UnionFind(n)
    pair_meta: dict[tuple[int, int], tuple[Confidence, str]] = {}

    for i in range(n):
        for j in range(i + 1, n):
            a, b = ungrouped[i], ungrouped[j]
            matched, conf = _names_match(a.business_name, b.business_name)
            if matched:
                uf.union(i, j)
                pair_meta[(i, j)] = (conf, _pair_reason(a, b))
                continue
            da, db_domain = _email_domain(a.primary_contact_email), _email_domain(
                b.primary_contact_email
            )
            if da and db_domain and da == db_domain:
                ta, tb = _name_tokens(a.business_name), _name_tokens(b.business_name)
                if ta & tb:
                    uf.union(i, j)
                    pair_meta[(i, j)] = (
                        "medium",
                        f"Shared contact domain @{da} with overlapping name tokens",
                    )

    clusters: dict[int, list[int]] = defaultdict(list)
    for i in range(n):
        clusters[uf.find(i)].append(i)

    out: list[dict] = []
    for indices in clusters.values():
        if len(indices) < 2:
            continue
        members = [ungrouped[i] for i in indices]
        pair_confidences: list[Confidence] = []
        reasons: list[str] = []
        for a_idx in range(len(indices)):
            for b_idx in range(a_idx + 1, len(indices)):
                i, j = indices[a_idx], indices[b_idx]
                key = (i, j) if i < j else (j, i)
                if key in pair_meta:
                    conf, reason = pair_meta[key]
                    pair_confidences.append(conf)
                    if reason not in reasons:
                        reasons.append(reason)

        display_name = max((m.business_name for m in members), key=len)
        confidence = _cluster_confidence(members, pair_confidences)
        reason = reasons[0] if reasons else "Ungrouped members with similar business names"

        out.append(
            {
                "suggested_display_name": display_name,
                "suggested_slug": slugify_display_name(display_name),
                "member_ids": [m.id for m in members],
                "members": [
                    {
                        "id": m.id,
                        "business_name": m.business_name,
                        "external_business_id": m.external_business_id,
                        "stage": m.stage,
                    }
                    for m in members
                ],
                "reason": reason,
                "confidence": confidence,
            }
        )

    out.sort(key=lambda c: (-len(c["member_ids"]), c["suggested_display_name"].lower()))
    return out


def delete_entity_group(db: Session, group: EntityGroup) -> int:
    """
    Unlink all CRM members from the group, then delete the group row.
    Returns the number of members that were unlinked.
    """
    unlinked_count = (
        db.query(Client)
        .filter(Client.entity_group_id == group.id)
        .update({Client.entity_group_id: None}, synchronize_session="fetch")
    )
    db.delete(group)
    db.commit()
    return unlinked_count
