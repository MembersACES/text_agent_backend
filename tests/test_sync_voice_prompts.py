"""Verify helpers for the Retell shared-block sync."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from sync_voice_prompts import (  # noqa: E402
    ARRANGE_A_CALL_PHRASE,
    BLOCK_RE,
    COMPARISON_HOLDING_PHRASE,
    HOLDING_TOKEN,
    LANE_COPY,
    NEW_TEXT_MARKERS,
    OLD_CONSULTANTS_WORDING,
    OLD_ESCALATION_SCRIPT,
    PAUSE_TOKEN,
    START,
    apply_block,
    assert_template_is_lane_neutral,
    block_diff_lines,
    body_diff_lines,
    build_parser,
    compose_shared_block,
    edit_state_label,
    format_verify_line,
    lane_for_sequence,
    leftover_superseded,
    managed_section,
    normalize_agent_ids,
    published_has_new_text,
    published_prompt_ok,
    rewrite_unmanaged_body,
    unmanaged_body,
    wrap_managed,
)


def test_apply_block_appends_then_replaces() -> None:
    first = apply_block("You are Alex.", "Amelia Williams at business@acesolutions.com.au")
    assert "ACES SHARED RULES" in first
    assert "Amelia Williams" in first
    second = apply_block(first, "Amelia Williams at business@acesolutions.com.au or 1300 849 908")
    assert second.count(START) == 1
    assert len(BLOCK_RE.findall(second)) == 1
    assert "1300 849 908" in second


def test_published_has_new_text_requires_all_markers() -> None:
    full = " ".join(NEW_TEXT_MARKERS)
    assert published_has_new_text(full) is True
    assert published_has_new_text("Amelia Williams only") is False
    assert published_has_new_text(OLD_CONSULTANTS_WORDING) is False


def test_verify_line_names_agent_version_and_whether_published_contains_new_text() -> None:
    prompt = (
        "I'll pass that to Amelia Williams at ACES. You can also email "
        "business@acesolutions.com.au or call 1300 849 908."
    )
    line = format_verify_line("agent_abc", 3, True, prompt)
    assert "agent_id=agent_abc" in line
    assert "version=3" in line
    assert "published=yes" in line
    assert "published contains new text: YES" in line

    old = format_verify_line("agent_abc", 0, True, "best handled by one of our consultants")
    assert "version=0" in old
    assert "published contains new text: NO" in old
    assert "FAIL superseded wording still present" in old


def test_published_prompt_ok_fails_when_new_text_sits_beside_consultants() -> None:
    prompt = (
        "I'll pass that to Amelia Williams at ACES. You can also email "
        "business@acesolutions.com.au or call 1300 849 908. "
        "That's a good question and it's best handled by one of our consultants."
    )
    assert published_has_new_text(prompt) is True
    assert leftover_superseded(prompt) == [OLD_CONSULTANTS_WORDING]
    assert published_prompt_ok(prompt) is False
    line = format_verify_line("agent_abc", 1, True, prompt)
    assert "published contains new text: YES" in line
    assert "FAIL superseded wording still present" in line


def test_only_accepts_one_or_more_agent_ids() -> None:
    parser = build_parser()
    one = parser.parse_args(["--only", "agent_campaign"])
    assert normalize_agent_ids(one.only) == ["agent_campaign"]
    many = parser.parse_args(["--only", "agent_a", "agent_b"])
    assert normalize_agent_ids(many.only) == ["agent_a", "agent_b"]
    csv = parser.parse_args(["--only", "agent_a,agent_b"])
    assert normalize_agent_ids(csv.only) == ["agent_a", "agent_b"]


def test_omitting_apply_is_a_dry_run_and_dry_run_flag_exists() -> None:
    parser = build_parser()
    default = parser.parse_args(["--from-db"])
    assert default.apply is False
    explicit = parser.parse_args(["--from-db", "--dry-run"])
    assert explicit.dry_run is True
    assert explicit.apply is False


def test_edit_state_distinguishes_frozen_from_draft() -> None:
    assert edit_state_label(True, 0, 0) == "FROZEN (published v0, no draft)"
    assert (
        edit_state_label(False, 1, 0)
        == "DRAFT v1 sitting on published v0"
    )


def test_block_diff_shows_old_against_new() -> None:
    old_prompt = apply_block("You are Alex.", "best handled by one of our consultants")
    lines = block_diff_lines(old_prompt, "Amelia Williams at business@acesolutions.com.au", "old", "new")
    text = "\n".join(lines)
    assert "managed block: CHANGE" in text
    assert "best handled by one of our consultants" in text
    assert "Amelia Williams" in text
    identical = block_diff_lines(old_prompt, "best handled by one of our consultants", "old", "new")
    assert identical == ["  managed block: identical"]
    assert managed_section(old_prompt).startswith(START)
    assert "consultants" in wrap_managed("consultants")


def test_lane_for_sequence_uses_campaign_only_for_gci() -> None:
    assert lane_for_sequence("gci_outbound_v1") == "campaign"
    assert lane_for_sequence("gas_followup_v1") == "comparison"
    assert lane_for_sequence(None) == "comparison"
    assert lane_for_sequence("gci_outbound_v1", override="comparison") == "comparison"
    assert lane_for_sequence(None, override="campaign") == "campaign"


def test_compose_campaign_does_not_claim_figures_or_expiry() -> None:
    template = (
        f"Point them at what they already have:\n\n{HOLDING_TOKEN}\n\n"
        f"{PAUSE_TOKEN}\n"
    )
    campaign = compose_shared_block(template, "campaign")
    assert "It's all in the email I sent through, have a look and I'll follow up with you" in campaign
    assert COMPARISON_HOLDING_PHRASE not in campaign
    assert "expiry" not in campaign.lower()
    assert "figures" not in campaign.lower()
    assert "a saving, a rate, an expiry" not in campaign

    comparison = compose_shared_block(template, "comparison")
    assert COMPARISON_HOLDING_PHRASE in comparison
    assert "Have a look and I'll follow up with you." in comparison
    assert "a saving, a rate, an expiry" in comparison


def test_shared_block_file_is_lane_neutral() -> None:
    path = Path(__file__).resolve().parents[1] / "scripts" / "voice_shared_block.md"
    text = path.read_text(encoding="utf-8")
    assert_template_is_lane_neutral(text)
    assert HOLDING_TOKEN in text
    assert PAUSE_TOKEN in text
    assert COMPARISON_HOLDING_PHRASE not in text
    campaign = compose_shared_block(text, "campaign")
    assert published_prompt_ok(campaign, lane="campaign") is True
    assert COMPARISON_HOLDING_PHRASE not in campaign
    comparison = compose_shared_block(text, "comparison")
    assert COMPARISON_HOLDING_PHRASE in comparison
    assert "Amelia Williams" in comparison


def test_campaign_verify_rejects_comparison_holding_copy() -> None:
    prompt = (
        "I'll pass that to Amelia Williams at ACES. You can also email "
        "business@acesolutions.com.au or call 1300 849 908. "
        "Everything's in the email I sent through - the figures and the expiry are all in there."
    )
    assert published_has_new_text(prompt) is True
    assert published_prompt_ok(prompt, lane="campaign") is False
    line = format_verify_line("agent_campaign", 1, True, prompt, lane="campaign")
    assert "FORBIDDEN campaign holding copy" in line


def test_lane_flag_on_parser() -> None:
    parser = build_parser()
    args = parser.parse_args(["--only", "agent_campaign", "--lane", "campaign", "--dry-run"])
    assert args.lane == "campaign"
    assert args.dry_run is True


CONSULTANTS_PARAGRAPH = (
    'Use: "That\'s a good question and it\'s best handled by one of our consultants. I\n'
    'can arrange a call or a short online meeting so they can answer properly."'
)


def test_rewrite_replaces_escalation_script_and_keeps_the_triggers() -> None:
    prompt = (
        "## Escalate to a human, do not continue selling\n"
        "Escalate if: they want exact pricing or the business is government, a hospital, "
        "a listed company or a large industrial site.\n"
        f"{CONSULTANTS_PARAGRAPH}\n\n"
        "## Capture if it comes up naturally\n"
        "Role and best contact number.\n"
    )
    rewritten, replaced = rewrite_unmanaged_body(prompt, "campaign")
    assert replaced == ["escalation_script"]
    assert "government, a hospital" in rewritten
    assert "Capture if it comes up naturally" in rewritten
    assert ARRANGE_A_CALL_PHRASE not in rewritten
    assert OLD_CONSULTANTS_WORDING not in rewritten
    assert "for a business your size" in rewritten
    assert "I'll pass this on to Amelia Williams at ACES" in rewritten
    assert "business@acesolutions.com.au" in rewritten
    assert "1300" not in rewritten
    assert leftover_superseded(rewritten) == []


def test_campaign_and_comparison_escalation_scripts_differ() -> None:
    campaign = LANE_COPY["campaign"]["escalation_script"]
    comparison = LANE_COPY["comparison"]["escalation_script"]
    assert "for a business your size" in campaign
    assert "for a business your size" not in comparison
    assert "business@acesolutions.com.au" in campaign
    assert "business@acesolutions.com.au" in comparison
    assert "1300" not in campaign
    assert "1300" not in comparison
    assert ARRANGE_A_CALL_PHRASE not in campaign
    assert ARRANGE_A_CALL_PHRASE not in comparison


def test_apply_block_replaces_escalation_in_body_even_when_managed_block_exists() -> None:
    shared = "Amelia Williams at business@acesolutions.com.au or 1300 849 908."
    first = apply_block(f"You are Alex.\n\n{CONSULTANTS_PARAGRAPH}", shared, "campaign")
    assert "ACES SHARED RULES" in first
    assert ARRANGE_A_CALL_PHRASE not in first
    assert "for a business your size" in first
    again = apply_block(first, shared, "campaign")
    assert again.count(START) == 1
    assert leftover_superseded(again) == []
    assert published_prompt_ok(again) is True


def test_body_diff_shows_escalation_wording_replaced_not_removed() -> None:
    current = (
        "Escalate if the business is a hospital.\n"
        f"{CONSULTANTS_PARAGRAPH}\n"
    )
    updated = apply_block(current, "Amelia Williams at business@acesolutions.com.au", "campaign")
    lines = body_diff_lines(current, updated, "old body", "new body")
    text = "\n".join(lines)
    assert "unmanaged body: CHANGE" in text
    assert "best handled by one of our consultants" in text
    assert "I'll pass this on to Amelia Williams at ACES" in text
    assert "hospital" in unmanaged_body(updated)
