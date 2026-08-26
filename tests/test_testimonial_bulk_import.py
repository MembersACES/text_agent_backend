"""Unit tests for testimonial zip → solution-type mapping (no network)."""

from tools.testimonial_bulk_import import (
    SkipReason,
    classify_zip_path,
    match_crm_member,
    member_hint_from_filename,
    plan_zip_entries,
    type_from_aaa_filename,
)
from tools.testimonial_solution_content import ALL_SOLUTION_TYPE_IDS, DEFAULT_CONTENT

NEW_TYPES = (
    "client_endorsement",
    "ghg_roadmap",
    "solar_review",
    "gas_discrepancy",
    "electricity_discrepancy",
    "demand_reset",
    "cds",
)


def test_new_types_are_allowlisted():
    for type_id in NEW_TYPES:
        assert type_id in ALL_SOLUTION_TYPE_IDS
        challenge = DEFAULT_CONTENT[type_id]["key_challenge_of_solution"]
        assert "Describe the challenge" not in challenge


def test_folder_maps_to_expected_types():
    cases = {
        "SERVICE /Frankston RSL minimalist Testimonial 04.06.png": "client_endorsement",
        "GHG/Darebin RSL minimalist ghg roadmap 04.06.png": "ghg_roadmap",
        "Solar Review /Healesville RSL Solar Review.png": "solar_review",
        "Gas Discrepancy /Copy of Testimonial_-_Frankston_RSL_-_Gas_Discrepancy_Recovery.docx": "gas_discrepancy",
        "Electricity Discrepancy /Masonic Club minimalist discrepancy adjust 04.06 (1).png": "electricity_discrepancy",
        "Demand Reset /Masonic Club minimalist max demand reset 04.06 (1).png": "demand_reset",
        "CDS /Frankston RSL minimalist CDS 04.06.png": "cds",
        "Electricity contract /North Melbourne minimalist E-C&I 10.06.png": "ci_electricity",
        "Gas Contract /Frankston RSL minimalist G-C&I 04.06.png": "ci_gas",
        "WASTE/Darebin RSL - Waste review-1.pdf": "waste",
        "Cooking Oil /Testimonial_-_Longbeach_RSL_-_Cooking_Oil_Review.docx": "resource_recovery",
        "DMA /Cheltenham RSL Step 1 (5-6) DMA.png": "dma",
        "Cleaning robot /Frankston RSL Automated Cleaning Robot Testimonial & Result.pdf": "automated_cleaning_robot",
        "Solar Cleaning /Richmond FC minimalist Solar cleaning 10.06.png": "solar_panel_cleaning",
    }
    for path, expected in cases.items():
        classified = classify_zip_path(path)
        assert classified.skip_reason is None, path
        assert classified.solution_type_id == expected, path


def test_skips_offers_story_new_layout_and_analysis():
    skips = {
        "Offers/Alinta 2026 Gas Agreement .pdf": SkipReason.NOT_TESTIMONIAL_FOLDER,
            "New Layout/Already Moved/Testimonial_-_Healesville_RSL_-_Solar_Review.docx": SkipReason.NOT_TESTIMONIAL_FOLDER,
        "AAA _ New Format 20260814/Story format/Healesville RSL testimonial (story) - Solar Review.docx": SkipReason.DUPLICATE_LAYOUT,
        "AAA _ New Format 20260814/Story format/~$x Hill RSL testimonial (story) - Sustainable Waste Review.docx": SkipReason.WORD_LOCK,
        "Cleaning robot /Frankston RSL Cleaning Robot Analysis 2025.09.3 (1).pdf": SkipReason.ANALYSIS_OR_PRESENTATION,
        "Cooking Oil /Cooking Oil Analysis Summary - Frankston RSL-1.pdf": SkipReason.ANALYSIS_OR_PRESENTATION,
        "New Layout/ACES-CZA_VIC_RSL_2026_Testimonial_Presentation_v2.pptx": SkipReason.UNSUPPORTED_EXTENSION,
        "latest draft vacuum mopping dryer bot (voice and subs).mp4": SkipReason.UNSUPPORTED_EXTENSION,
    }
    for path, reason in skips.items():
        classified = classify_zip_path(path)
        assert classified.skip_reason == reason, (path, classified.skip_reason)


def test_aaa_suffixes():
    assert type_from_aaa_filename("Frankston RSL testimonial - Gas Billing Discrepancy Recovery.docx") == "gas_discrepancy"
    assert type_from_aaa_filename("Cheltenham RSL testimonial - GHG Roadmap and Cooking Oil.docx") == "ghg_roadmap"
    assert type_from_aaa_filename("Longbeach RSL testimonial - Client Endorsement.docx") == "client_endorsement"
    assert type_from_aaa_filename("Healesville RSL testimonial - Solar Review.docx") == "solar_review"
    assert type_from_aaa_filename("Swin Alumni (Geelong & Surfcoast Laundry) testimonial - Gas Billing Discrepancy Recovery.docx") == "gas_discrepancy"


def test_member_hints():
    assert "Frankston" in member_hint_from_filename("Frankston RSL minimalist CDS 04.06.png")
    assert "Healesville" in member_hint_from_filename("Healesville RSL Solar Review.png")
    assert "Darebin" in member_hint_from_filename("Testimonial - Solar Panel Cleaning - Darebin RSL Sub Branch Inc.pdf")
    assert "swin" in member_hint_from_filename("Testimonial - SWIN ALUMNI PTY LTD (1).pdf").lower()


def test_prefers_aaa_word_over_png_and_copy_of():
    planned = plan_zip_entries(
        [
            "Solar Review /Healesville RSL Solar Review.png",
            "Solar Review /Healesville RSL Solar Review .png",
            "Solar Review /Copy of Testimonial_-_Healesville_RSL_-_Solar_Review.docx",
            "AAA _ New Format 20260814/Healesville RSL testimonial - Solar Review.docx",
        ]
    )
    kept = [e for e in planned if e.preferred]
    assert len(kept) == 1
    assert kept[0].zip_path.startswith("AAA")
    assert kept[0].solution_type_id == "solar_review"


def test_groups_filename_variants_of_same_member():
    planned = plan_zip_entries(
        [
            "AAA _ New Format 20260814/Coolan Nominees testimonial - Gas Contract Review.docx",
            "Gas Contract /Testimonial - COOLAN NOMINEES PTY LTD - Google Docs.pdf",
            "AAA _ New Format 20260814/Gosford Sailing Club testimonial - Direct Metering Agreement.docx",
            "DMA /Testimonial for Gosford Sailing Club Ltd - DMA.pdf",
            "AAA _ New Format 20260814/Echuca Moama RSL testimonial - Gas Contract Review.docx",
            "Gas Contract /Testimonial - Echuca Moama RSL & Citizens Club.docx.pdf",
        ]
    )
    kept = [e for e in planned if e.preferred]
    assert len(kept) == 3
    assert all(e.zip_path.startswith("AAA") for e in kept)


def test_darebin_waste_pdf_is_duplicate_of_aaa_word():
    planned = plan_zip_entries(
        [
            "WASTE/Darebin RSL - Waste review-1.pdf",
            "AAA _ New Format 20260814/Darebin RSL testimonial - Sustainable Waste Review.docx",
        ]
    )
    kept = [e for e in planned if e.preferred]
    assert len(kept) == 1
    assert kept[0].zip_path.startswith("AAA")


def test_crm_match_uses_exact_business_name():
    from tools.testimonial_bulk_import import CrmClient

    clients = [
        CrmClient("Frankston RSL Sub-Branch Inc.", "https://drive.google.com/drive/folders/abc"),
        CrmClient("Box Hill RSL Sub-Branch Inc."),
        CrmClient("Darebin RSL Sub Branch Inc"),
    ]
    match = match_crm_member("Frankston RSL", clients)
    assert match is not None
    assert match.business_name.startswith("Frankston")
    assert match.ambiguous is False
    assert match.gdrive_folder_url


def test_skips_existing_crm_and_sheet_types():
    from tools.testimonial_bulk_import import (
        ExistingTestimonial,
        existing_from_api_rows,
        type_already_on_member,
    )

    name = "Frankston RSL Sub Branch Inc"
    existing = existing_from_api_rows(
        [
            {
                "business_name": name,
                "testimonial_solution_type_id": "automated_cleaning_robot",
                "testimonial_type": "Automated Cleaning Robot",
            },
            {
                "business_name": name,
                "testimonial_solution_type_id": None,
                "testimonial_type": "Oil / Resource Recovery",
            },
            {
                "business_name": name,
                "testimonial_solution_type_id": None,
                "testimonial_type": "C&I Gas Reviews",
            },
        ],
        fallback_business_name=name,
    )
    assert type_already_on_member(name, "automated_cleaning_robot", existing)
    assert type_already_on_member(name, "resource_recovery", existing)
    assert type_already_on_member(name, "ci_gas", existing)
    assert not type_already_on_member(name, "waste", existing)
    assert not type_already_on_member("Darebin RSL Sub Branch Inc", "ci_gas", existing)
    assert not type_already_on_member(
        name,
        "waste",
        [ExistingTestimonial(business_name=name, solution_type_id="ci_electricity")],
    )


def test_colleague_zip_maps_new_types_when_present():
    from pathlib import Path
    import zipfile

    zip_path = Path.home() / "Downloads" / "drive-download-20260824T231613Z-1-001.zip"
    if not zip_path.is_file():
        return
    with zipfile.ZipFile(zip_path) as archive:
        planned = plan_zip_entries(archive.namelist())
    kept = [e for e in planned if e.preferred]
    types = {e.solution_type_id for e in kept}
    for type_id in NEW_TYPES:
        assert type_id in types, type_id
    skipped_paths = " ".join(e.zip_path.lower() for e in planned if e.skip_reason)
    assert "offers/" in skipped_paths
    assert not any(e.zip_path.lower().startswith("offers/") and e.preferred for e in planned)
    assert all(e.member_hint for e in kept)
