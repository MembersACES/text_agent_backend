from enum import Enum


class ClientStage(str, Enum):
    LEAD = "lead"
    QUALIFIED = "qualified"
    LOA_SIGNED = "loa_signed"
    DATA_COLLECTED = "data_collected"
    ANALYSIS_IN_PROGRESS = "analysis_in_progress"
    OFFER_SENT = "offer_sent"
    WON = "won"
    EXISTING_CLIENT = "existing_client"  # Ongoing customer; used for future transitions
    LOST = "lost"


# Stages that represent an already-won/existing customer. Used to avoid downgrading
# when an existing client receives another accepted offer.
POST_WIN_STAGES = (ClientStage.WON, ClientStage.EXISTING_CLIENT)


class OfferStatus(str, Enum):
    REQUESTED = "requested"
    AWAITING_RESPONSE = "awaiting_response"
    RESPONSE_RECEIVED = "response_received"
    ACCEPTED = "accepted"
    LOST = "lost"


class OfferActivityType(str, Enum):
    """Structured activity types for offer artefacts. Constrained so we don't send arbitrary strings."""
    QUOTE_REQUEST = "quote_request"
    BASE2_REVIEW = "base2_review"
    COMPARISON = "comparison"
    GHG_OFFER = "ghg_offer"
    ENGAGEMENT_FORM = "engagement_form"
    DISCREPANCY_EMAIL_SENT = "discrepancy_email_sent"

