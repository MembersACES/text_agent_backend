"""Matching LinkedIn draft filenames to testimonial rows, and reading .docx text."""

import io
import zipfile

from tools.social_drafts import (
    assign_draft_files,
    docx_bytes_to_text,
    is_post_guide,
    normalise_party_name,
)


def _docx(paragraphs: list[str]) -> bytes:
    ns = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
    body = "".join(
        f'<w:p><w:r><w:t xml:space="preserve">{paragraph}</w:t></w:r></w:p>'
        for paragraph in paragraphs
    )
    xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<w:document xmlns:w="{ns}"><w:body>{body}</w:body></w:document>'
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("word/document.xml", xml)
    return buffer.getvalue()


def test_docx_text_keeps_paragraphs():
    text = docx_bytes_to_text(_docx(["Bentleigh RSL cut its electricity spend.", "A short second line."]))
    assert text == "Bentleigh RSL cut its electricity spend.\n\nA short second line."


def test_normalise_strips_testimonial_prefix_and_legal_suffix():
    assert normalise_party_name("Testimonial_Bentleigh RSL.docx") == "bentleigh rsl"
    assert normalise_party_name("Testimonial_ Bentleigh RSL.docx") == "bentleigh rsl"
    assert normalise_party_name("Testimonal_ Longbeach RSL.docx") == "longbeach rsl"
    assert normalise_party_name("Bentleigh RSL Sub Branch") == "bentleigh rsl"
    assert normalise_party_name("Licola JMC Pty Ltd") == "licola jmc"
    assert normalise_party_name("Longbeach RSL Sub Branch Inc") == "longbeach rsl"
    assert normalise_party_name("Peninsula Villages Limited") == "peninsula villages"


def test_known_linkedin_files_match_member_rows():
    files = [
        {"id": "bent", "name": "Testimonial_Bentleigh RSL.docx"},
        {"id": "lic", "name": "Testimonial_Licola JMC.docx"},
        {"id": "long", "name": "Testimonial_Longbeach RSL.docx"},
        {"id": "pen", "name": "Testimonial_Peninsula Villages.docx"},
    ]
    rows = [
        {"id": 1, "business_name": "Bentleigh RSL Sub Branch", "status": "Approved", "social_status": "Drafted"},
        {"id": 2, "business_name": "Licola JMC Pty Ltd", "status": "Approved", "social_status": "Drafted"},
        {"id": 3, "business_name": "Longbeach RSL Sub Branch Inc", "status": "Approved", "social_status": "Drafted"},
        {"id": 4, "business_name": "Peninsula Villages Limited", "status": "Approved", "social_status": "Drafted"},
        {"id": 5, "business_name": "Moama Bowling Club", "status": "Approved", "social_status": None},
    ]
    matches, unlinked = assign_draft_files(files, rows)
    paired = {match["business_name"]: match["file"]["id"] for match in matches}
    assert paired == {
        "Bentleigh RSL Sub Branch": "bent",
        "Licola JMC Pty Ltd": "lic",
        "Longbeach RSL Sub Branch Inc": "long",
        "Peninsula Villages Limited": "pen",
    }
    assert unlinked == []
    assert is_post_guide("LinkedIn post guide (1).docx")
    assert not is_post_guide("Testimonial_Bentleigh RSL.docx")


def test_stored_file_id_wins_over_a_renamed_file():
    files = [
        {"id": "file-a", "name": "Testimonial_Someone Else.docx"},
        {"id": "file-b", "name": "Testimonial_Bentleigh RSL.docx"},
    ]
    rows = [
        {
            "id": 1,
            "business_name": "Bentleigh RSL Sub Branch",
            "status": "Approved",
            "social_status": "Drafted",
            "social_draft_file_id": "file-a",
        }
    ]
    matches, unlinked = assign_draft_files(files, rows)
    assert matches[0]["file"]["id"] == "file-a"
    assert [file["id"] for file in unlinked] == ["file-b"]


def test_one_file_is_not_attached_to_two_rows():
    files = [{"id": "bent", "name": "Testimonial_Bentleigh RSL.docx"}]
    rows = [
        {"id": 1, "business_name": "Bentleigh RSL Sub Branch", "status": "Approved", "social_status": None},
        {"id": 2, "business_name": "Bentleigh RSL Sub Branch", "status": "Approved", "social_status": "Drafted"},
    ]
    matches, unlinked = assign_draft_files(files, rows)
    assert len(matches) == 1
    assert matches[0]["testimonial_id"] == 2
    assert unlinked == []


def test_unrecognised_file_stays_unlinked():
    files = [{"id": "x", "name": "Testimonial_Unknown Club.docx"}]
    matches, unlinked = assign_draft_files(files, [{"id": 1, "business_name": "Peninsula Villages Limited", "status": "Approved"}])
    assert matches == []
    assert unlinked[0]["id"] == "x"
