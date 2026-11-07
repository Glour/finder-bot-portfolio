import json


def _load_json(file_path: str) -> dict:
    """Load JSON file using context manager to ensure file handle is closed."""
    with open(file_path, "r") as f:
        return json.load(f)


lbank_price_tick_info = _load_json("infrastructure/services/collectors/price_collector/supports/lbank_price_tick.json")
