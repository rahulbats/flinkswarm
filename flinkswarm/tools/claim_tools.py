"""Claim data tools. The Cosmos DB call is mocked; swap `_fetch` for a real
azure-cosmos client when wiring the live backend.
"""

from __future__ import annotations

from .registry import register

_FAKE_CLAIMS: dict[str, dict] = {
    "CLM-1001": {
        "claim_id": "CLM-1001",
        "claimant": "A. Rivera",
        "policy_id": "POL-55",
        "date_of_loss": "2026-07-14",
        "peril": "water damage",
        "amount_claimed": 8200.00,
        "line_items": [
            {"desc": "hardwood floor replacement", "amount": 6100.00},
            {"desc": "drywall + paint", "amount": 2100.00},
        ],
        "adjuster_notes": "Supply line burst under kitchen sink. Photos on file.",
    },
    "CLM-1002": {
        "claim_id": "CLM-1002",
        "claimant": "J. Okafor",
        "policy_id": "POL-91",
        "date_of_loss": "2026-08-02",
        "peril": "flood",
        "amount_claimed": 15000.00,
        "line_items": [{"desc": "basement contents", "amount": 15000.00}],
        "adjuster_notes": "Regional flooding event. No separate flood policy on file.",
    },
}


@register(
    name="get_claim_from_cosmos",
    description="Retrieve the full claim record for a claim_id from the claims store.",
    parameters={
        "type": "object",
        "properties": {"claim_id": {"type": "string", "description": "e.g. CLM-1001"}},
        "required": ["claim_id"],
    },
)
def get_claim_from_cosmos(claim_id: str) -> dict:
    return _FAKE_CLAIMS.get(
        claim_id,
        {"claim_id": claim_id, "error": "claim not found"},
    )
