from enum import Enum


class ClientStage(str, Enum):
    """
    Coarse client lifecycle focused on the overall relationship, not offer micro-steps.
    Existing rows that still use legacy granular stages are normalised when read.
    """

    LEAD = "lead"
    QUALIFIED = "qualified"
    WON = "won"
    EXISTING_CLIENT = "existing_client"  # Ongoing customer; used for future transitions
    LOST = "lost"


# Stages that represent an already-won/existing customer. Used to avoid downgrading
# when an existing client receives another accepted offer.
POST_WIN_STAGES = (ClientStage.WON, ClientStage.EXISTING_CLIENT)


class OfferPipelineStage(str, Enum):
    """
    Detailed, mostly-linear pipeline for a single offer.

    This is intentionally separate from OfferStatus (requested/accepted/lost) so we
    can show granular progress through the comparison → engagement → contract flow
    without overloading status with every micro-step.
    """

    COMPARISON_SENT = "comparison_sent"
    ENGAGEMENT_FORM_SENT = "engagement_form_sent"
    ENGAGEMENT_FORM_SIGNED = "engagement_form_signed"
    CONTRACT_REQUESTED = "contract_requested"
    CONTRACT_RECEIVED = "contract_received"
    CONTRACT_SENT_FOR_SIGNING = "contract_sent_for_signing"
    CONTRACT_SIGNED_LODGED = "contract_signed_lodged"
    CONTRACT_ACCEPTED = "contract_accepted"
    LOST = "lost"


class OfferStatus(str, Enum):
    REQUESTED = "requested"
    AWAITING_RESPONSE = "awaiting_response"
    RESPONSE_RECEIVED = "response_received"
    ACCEPTED = "accepted"
    LOST = "lost"


class OfferActivityType(str, Enum):
    """Structured activity types for offer artefacts. Constrained so we don't send arbitrary strings."""
    QUOTE_REQUEST = "quote_request"
    DATA_REQUEST = "data_request"
    BASE2_REVIEW = "base2_review"
    COMPARISON = "comparison"
    GHG_OFFER = "ghg_offer"
    ENGAGEMENT_FORM = "engagement_form"
    ENGAGEMENT_FORM_SIGNED = "engagement_form_signed"
    CONTRACT_REQUESTED = "contract_requested"
    CONTRACT_RECEIVED = "contract_received"
    CONTRACT_SENT_FOR_SIGNING = "contract_sent_for_signing"
    CONTRACT_SIGNED_LODGED = "contract_signed_lodged"
    DISCREPANCY_EMAIL_SENT = "discrepancy_email_sent"
    DMA_REVIEW_GENERATED = "dma_review_generated"
    DMA_EMAIL_SENT = "dma_email_sent"
    # User-added from offer page (no pipeline/status side effects)
    MANUAL_DOCUMENT = "manual_document"
    MANUAL_ACTIVITY = "manual_activity"

