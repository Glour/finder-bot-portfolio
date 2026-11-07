import json
from pathlib import Path

from infrastructure.services.collectors.structures.normalizer import get_chain_normalizer


def _load_json(filename: str):
    """Load bundled exchange metadata independently of the working directory."""
    with (Path(__file__).parent / filename).open(encoding="utf-8") as f:
        return json.load(f)


gate_chain_fee_info = _load_json("gate_fees.json")
lbank_contract_addresses = _load_json("lbank_contract_addresses.json")
chain_normalizer_instance = get_chain_normalizer()
