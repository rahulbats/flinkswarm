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
    "CLM-1003": {
        "claim_id": "CLM-1003",
        "claimant": "M. Chen",
        "policy_id": "POL-55",
        "date_of_loss": "2026-08-19",
        "peril": "fire",
        "amount_claimed": 42000.00,
        "line_items": [
            {"desc": "kitchen rebuild (cabinets, counters, appliances)", "amount": 31000.00},
            {"desc": "smoke remediation, whole home", "amount": 11000.00},
        ],
        "adjuster_notes": "Grease fire on stovetop, contained to kitchen. Fire dept report on file. No accelerants.",
    },
    "CLM-1004": {
        "claim_id": "CLM-1004",
        "claimant": "T. Alvarez",
        "policy_id": "POL-55",
        "date_of_loss": "2026-09-01",
        "peril": "wind",
        "amount_claimed": 9800.00,
        "line_items": [
            {"desc": "detached garage roof replacement", "amount": 9800.00},
        ],
        "adjuster_notes": "Windstorm tore shingles off the detached garage. Main dwelling undamaged.",
    },
    "CLM-1005": {
        "claim_id": "CLM-1005",
        "claimant": "R. Novak",
        "policy_id": "POL-55",
        "date_of_loss": "2026-06-30",
        "peril": "water damage",
        "amount_claimed": 12500.00,
        "line_items": [
            {"desc": "subfloor + joist repair, bathroom", "amount": 8500.00},
            {"desc": "mold remediation", "amount": 4000.00},
        ],
        "adjuster_notes": "Long-running leak from a corroded shower drain. Staining and rot indicate months of exposure; homeowner reported knowing the shower 'ran slow' since spring.",
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
