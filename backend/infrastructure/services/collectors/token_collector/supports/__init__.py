import json
from pathlib import Path

from infrastructure.services.collectors.structures.normalizer import get_chain_normalizer


def _load_json(file_path: str) -> dict:
    """Load JSON file using context manager to ensure file handle is closed."""
    with open(file_path, "r") as f:
        return json.load(f)


gate_chain_fee_info = _load_json("infrastructure/services/collectors/token_collector/supports/gate_fees.json")
lbank_contract_addresses = _load_json("infrastructure/services/collectors/token_collector/supports/lbank_contract_addresses.json")
chain_normalizer_instance = get_chain_normalizer()
