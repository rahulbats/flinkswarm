"""Policy document tools. The Blob storage call is mocked; swap `_fetch` for a
real azure-storage-blob client when wiring the live backend.
"""

from __future__ import annotations

from .registry import register

_FAKE_POLICIES: dict[str, dict] = {
    "CLM-1001": {
        "policy_id": "POL-55",
        "form": "HO-3",
        "dwelling_limit": 350000.00,
        "deductible": 1000.00,
        "covered_perils": ["fire", "water damage (sudden & accidental)", "wind", "theft"],
        "exclusions": ["flood", "earth movement", "wear and tear", "neglect"],
        "clauses": {
            "SEC-II-A": "Sudden and accidental discharge of water from a plumbing system is covered.",
            "SEC-IV-3": "Losses caused by flood, surface water, or overflow of a body of water are excluded.",
        },
    },
    "CLM-1002": {
        "policy_id": "POL-91",
        "form": "HO-3",
        "dwelling_limit": 220000.00,
        "deductible": 2500.00,
        "covered_perils": ["fire", "wind", "theft"],
        "exclusions": ["flood", "surface water", "earth movement"],
        "clauses": {
            "SEC-IV-3": "Flood and surface water damage are excluded unless a separate flood endorsement is in force.",
        },
    },
    # CLM-1003/1004/1005 are all on POL-55, but each record carries the clauses
    # and sublimits relevant to that loss (the real blob store would return the
    # whole policy; this keeps the mock focused).
    "CLM-1003": {
        "policy_id": "POL-55",
        "form": "HO-3",
        "dwelling_limit": 350000.00,
        "deductible": 1000.00,
        "covered_perils": ["fire", "water damage (sudden & accidental)", "wind", "theft"],
        "exclusions": ["flood", "earth movement", "wear and tear", "neglect", "intentional acts"],
        "clauses": {
            "SEC-I-A": "Fire and lightning are covered perils for the dwelling and its contents.",
            "SEC-IV-1": "Loss caused intentionally by an insured, or involving the use of accelerants, is excluded.",
        },
    },
    "CLM-1004": {
        "policy_id": "POL-55",
        "form": "HO-3",
        "dwelling_limit": 350000.00,
        "deductible": 1000.00,
        "covered_perils": ["fire", "water damage (sudden & accidental)", "wind", "theft"],
        "exclusions": ["flood", "earth movement", "wear and tear", "neglect"],
        "sublimits": {"other structures": 5000.00},
        "clauses": {
            "SEC-I-B": "Wind and hail are covered perils.",
            "SEC-I-C": "Coverage for other structures not attached to the dwelling "
            "(detached garage, shed, fence) is limited to $5,000 per occurrence.",
        },
    },
    "CLM-1005": {
        "policy_id": "POL-55",
        "form": "HO-3",
        "dwelling_limit": 350000.00,
        "deductible": 1000.00,
        "covered_perils": ["fire", "water damage (sudden & accidental)", "wind", "theft"],
        "exclusions": ["flood", "earth movement", "wear and tear", "neglect", "mold", "gradual deterioration"],
        "clauses": {
            "SEC-II-A": "Sudden and accidental discharge of water from a plumbing system is covered.",
            "SEC-IV-5": "Loss from continuous or repeated seepage over weeks or months, "
            "and any resulting mold, is excluded. Damage the insured knew about and failed "
            "to mitigate is not covered (neglect).",
        },
    },
}


@register(
    name="get_policy_from_blob",
    description="Retrieve the governing policy document (coverages, limits, exclusions) for a claim_id.",
    parameters={
        "type": "object",
        "properties": {"claim_id": {"type": "string", "description": "e.g. CLM-1001"}},
        "required": ["claim_id"],
    },
)
def get_policy_from_blob(claim_id: str) -> dict:
    return _FAKE_POLICIES.get(
        claim_id,
        {"claim_id": claim_id, "error": "policy not found"},
    )
