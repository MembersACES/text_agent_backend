"""Progress steps for Interface video creation (pack + CRM registration)."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PACK_STEP_LABELS = {
    "crm_drafts": "Video drafts registered in CRM",
    "pack_folder": "Drive pack folder created",
    "pack_subfolders": "Subfolders created (qa / scripts / slides / renders)",
    "pack_manifest": "pack-manifest.json uploaded",
    "pack_readme": "README.txt uploaded",
    "pipeline_queued": "Render pipeline queued",
}

RENDER_PIPELINE_STEPS: List[tuple[str, str]] = [
    ("pipeline_queued", "Render pipeline queued"),
    ("download_source", "Testimonial document downloaded from Drive"),
    ("parse_document", "Reading testimonial document"),
    ("analyze_content", "Extracting fields and slide plan"),
    ("write_presentation", "Writing testimonial JSON and narration"),
    ("manifest_updated", "Render manifest updated"),
    ("voice_created", "Voiceover generated"),
    ("rendering", "Rendering long + 30s MP4s (several minutes)"),
    ("qa_report", "QA review report generated"),
    ("publishing", "Uploading MP4s and QA to Drive pack"),
    ("complete", "Ready to review in Interface"),
]

# Legacy step id from earlier builds — map to write_presentation when merging.
LEGACY_STEP_ALIASES = {"presentation_built": "write_presentation"}

STEP_HINTS: Dict[str, str] = {
    "download_source": "Usually a few seconds from Google Drive.",
    "parse_document": "Opening the PDF or Word file locally.",
    "analyze_content": "Can take 1–3 minutes for scanned PDFs (OCR).",
    "write_presentation": "Building slide plan, narration, and JSON.",
    "voice_created": "Generating TTS audio for each scene.",
    "rendering": "Chrome + Remotion — often 5–15 minutes.",
    "qa_report": "Running creative QA checks on this video.",
    "publishing": "Uploading MP4s, scripts, and QA to the pack folder.",
}

STEP_ORDER = list(PACK_STEP_LABELS.keys()) + [sid for sid, _ in RENDER_PIPELINE_STEPS if sid not in PACK_STEP_LABELS]


def _default_claude_videos_root() -> str:
    sibling = Path(__file__).resolve().parent.parent.parent / "claude-videos"
    return str(sibling) if sibling.is_dir() else ""


def _normalize_step_id(step_id: Optional[str]) -> Optional[str]:
    if not step_id:
        return step_id
    return LEGACY_STEP_ALIASES.get(step_id, step_id)


def infer_current_step(steps: List[Dict[str, Any]]) -> Optional[str]:
    for step in steps:
        if step.get("status") == "running":
            return _normalize_step_id(step.get("id"))
    for step in steps:
        if step.get("status") == "failed":
            return _normalize_step_id(step.get("id"))
    for step in reversed(steps):
        if step.get("status") == "done":
            return _normalize_step_id(step.get("id"))
    return _normalize_step_id(steps[0].get("id")) if steps else None


def _read_runner_meta(slug: str, videos_root: Optional[str] = None) -> Optional[Dict[str, Any]]:
    slug_val = (slug or "").strip().lower()
    if not slug_val:
        return None
    root = (videos_root or os.getenv("CLAUDE_VIDEOS_ROOT") or _default_claude_videos_root()).strip()
    if not root:
        return None
    path = Path(root) / ".pipeline-progress" / f"{slug_val}.runner.json"
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _runner_pid_alive(slug: str, videos_root: Optional[str] = None) -> bool:
    meta = _read_runner_meta(slug, videos_root)
    if not meta:
        return False
    pid = int(meta.get("pid") or 0)
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            import ctypes

            handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
            if handle:
                ctypes.windll.kernel32.CloseHandle(handle)
                return True
            return False
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def progress_is_stale(
    progress: Optional[Dict[str, Any]],
    *,
    slug: Optional[str] = None,
    max_age_seconds: int = 600,
) -> bool:
    """True when a running step has not updated recently or the pipeline subprocess is gone."""
    if not progress:
        return False
    has_running = any(s.get("status") == "running" for s in (progress.get("steps") or []))
    if not has_running:
        return False
    slug_val = (slug or progress.get("slug") or "").strip().lower()
    if slug_val and not _runner_pid_alive(slug_val):
        return True
    raw = (progress.get("updated_at") or "").strip()
    if not raw:
        return False
    try:
        ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - ts).total_seconds()
        return age > max_age_seconds
    except ValueError:
        return False


def read_pipeline_progress_file(slug: str, videos_root: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Read `.pipeline-progress/{slug}.json` written by run_testimonial_pipeline."""
    slug_val = (slug or "").strip().lower()
    if not slug_val:
        return None
    root = (videos_root or os.getenv("CLAUDE_VIDEOS_ROOT") or _default_claude_videos_root()).strip()
    if not root:
        return None
    path = Path(root) / ".pipeline-progress" / f"{slug_val}.json"
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def message_from_progress(progress: Optional[Dict[str, Any]]) -> Optional[str]:
    if not progress:
        return None
    if progress.get("stale") and progress.get("stale_message"):
        return str(progress["stale_message"])
    failed = progress_failure(progress)
    if failed:
        _step_id, msg = failed
        return msg
    current = progress.get("current_step")
    for step in progress.get("steps") or []:
        if step.get("id") == current or _normalize_step_id(step.get("id")) == _normalize_step_id(current):
            label = step.get("label") or ""
            detail = (step.get("detail") or "").strip()
            if step.get("status") == "running":
                return f"{label} — {detail}" if detail else label
            if step.get("status") == "done" and detail:
                return f"{label} — {detail}"
            return label or None
    return None


def progress_failure(progress: Optional[Dict[str, Any]]) -> Optional[Tuple[str, str]]:
    """Return (step_id, user-facing error) when a pipeline step failed."""
    if not progress:
        return None
    for step in progress.get("steps") or []:
        if step.get("status") == "failed":
            label = step.get("label") or step.get("id") or "Pipeline step"
            detail = (step.get("detail") or "").strip()
            return step.get("id") or "unknown", f"{label} failed{': ' + detail if detail else ''}"
    return None


def progress_is_complete(progress: Optional[Dict[str, Any]]) -> bool:
    if not progress:
        return False
    for step in progress.get("steps") or []:
        if step.get("id") == "complete" and step.get("status") == "done":
            return True
    return progress.get("current_step") == "complete"


def build_render_steps_skeleton(
    current_step: str = "download_source",
    *,
    active: bool = False,
) -> List[Dict[str, Any]]:
    """Placeholder render steps. Only mark a step running when `active` (real job/file progress)."""
    steps: List[Dict[str, Any]] = []
    for sid, label in RENDER_PIPELINE_STEPS:
        if sid == "pipeline_queued":
            continue
        status = "running" if active and sid == current_step else "pending"
        steps.append({"id": sid, "label": label, "status": status})
    return steps


def merge_progress(
    base_progress: Optional[Dict[str, Any]],
    overlay_progress: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Merge step lists by id; overlay wins for status/detail."""
    base_progress = base_progress or {}
    overlay_progress = overlay_progress or {}
    by_id: Dict[str, Dict[str, Any]] = {}
    for step in base_progress.get("steps") or []:
        sid = step.get("id")
        if sid:
            by_id[sid] = dict(step)
    for step in overlay_progress.get("steps") or []:
        sid = step.get("id")
        if not sid:
            continue
        sid = LEGACY_STEP_ALIASES.get(sid, sid)
        step = {**step, "id": sid}
        prev = by_id.get(sid, {})
        merged = {**prev, **{k: v for k, v in step.items() if v is not None}}
        by_id[sid] = merged

    ordered: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for sid in STEP_ORDER:
        if sid in by_id:
            ordered.append(by_id[sid])
            seen.add(sid)
    for sid, step in by_id.items():
        if sid not in seen:
            ordered.append(step)

    current = overlay_progress.get("current_step") or base_progress.get("current_step")
    current = _normalize_step_id(current)
    updated_at = overlay_progress.get("updated_at") or base_progress.get("updated_at")
    merged = {"current_step": infer_current_step(ordered) or current, "steps": ordered, "updated_at": updated_at}
    slug_for_stale = (overlay_progress.get("slug") or base_progress.get("slug") or "").strip().lower()
    if progress_is_stale(merged, slug=slug_for_stale or None):
        merged["stale"] = True
        merged["stale_message"] = (
            "The render pipeline stopped or stalled. "
            "Check that `npm run api:dev` is running on port 8001 (without --reload), then click Restart pipeline."
        )
    return merged


def build_running_progress_from_sources(
    slug: str,
    *,
    job_progress: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Progress for render-job poll — prefer on-disk pipeline file over placeholders."""
    file_progress = read_pipeline_progress_file(slug)
    if file_progress:
        return merge_progress(job_progress, file_progress)
    if job_progress and job_progress.get("steps"):
        return job_progress
    return {
        "current_step": "download_source",
        "steps": build_render_steps_skeleton(active=False),
        "updated_at": None,
    }


def build_pack_progress_steps(
    pack: Dict[str, Any],
    *,
    render_job: Optional[Dict[str, Any]] = None,
    crm_registered: bool = True,
    slug: Optional[str] = None,
) -> Dict[str, Any]:
    """Build initial progress snapshot returned from start-from-testimonial."""
    steps: List[Dict[str, Any]] = []
    current = "pipeline_queued"

    if crm_registered:
        steps.append({"id": "crm_drafts", "label": PACK_STEP_LABELS["crm_drafts"], "status": "done"})

    if pack.get("error"):
        steps.append(
            {
                "id": "pack_folder",
                "label": PACK_STEP_LABELS["pack_folder"],
                "status": "failed",
                "detail": str(pack.get("error"))[:200],
            }
        )
        current = "pack_folder"
        return {"current_step": current, "steps": steps}

    if pack.get("folder_url"):
        steps.append({"id": "pack_folder", "label": PACK_STEP_LABELS["pack_folder"], "status": "done"})
        name = (pack.get("folder_name") or "").strip()
        if name:
            steps[-1]["detail"] = name

    subfolders = pack.get("subfolders") or {}
    if subfolders:
        steps.append({"id": "pack_subfolders", "label": PACK_STEP_LABELS["pack_subfolders"], "status": "done"})

    warnings = pack.get("warnings") or []
    manifest_ok = not any("pack-manifest" in w for w in warnings)
    steps.append(
        {
            "id": "pack_manifest",
            "label": PACK_STEP_LABELS["pack_manifest"],
            "status": "done" if manifest_ok else "failed",
        }
    )
    readme_ok = not any("README" in w for w in warnings)
    steps.append(
        {
            "id": "pack_readme",
            "label": PACK_STEP_LABELS["pack_readme"],
            "status": "done" if readme_ok else "failed",
        }
    )

    job_status = (render_job or {}).get("status") or ""
    slug_val = (slug or "").strip().lower()
    file_progress = read_pipeline_progress_file(slug_val) if slug_val else None
    has_live = bool(file_progress or (render_job or {}).get("progress"))
    job_progress = merge_progress(
        {
            "steps": build_render_steps_skeleton("download_source", active=has_live),
            "current_step": "download_source" if has_live else None,
        },
        merge_progress(render_job.get("progress") if render_job else None, file_progress),
    )

    if job_status in ("failed", "enqueue_failed", "spawn_failed"):
        steps.append(
            {
                "id": "pipeline_queued",
                "label": PACK_STEP_LABELS["pipeline_queued"],
                "status": "failed",
                "detail": (render_job or {}).get("error") or (render_job or {}).get("message"),
            }
        )
        current = "pipeline_queued"
    elif job_status in ("queued", "running", "started_local"):
        steps.append({"id": "pipeline_queued", "label": PACK_STEP_LABELS["pipeline_queued"], "status": "done"})
        render_steps = [s for s in (job_progress.get("steps") or []) if s.get("id") != "pipeline_queued"]
        if not render_steps:
            render_steps = build_render_steps_skeleton(active=has_live)
        steps.extend(render_steps)
        current = job_progress.get("current_step") or ("download_source" if has_live else "download_source")
    else:
        steps.append({"id": "pipeline_queued", "label": PACK_STEP_LABELS["pipeline_queued"], "status": "pending"})

    return {"current_step": current, "steps": steps}
