"""
Pytest configuration and fixtures for backend tests.
"""
import sys
from pathlib import Path

import pytest

# Add backend to path for imports
backend_path = Path(__file__).parent.parent
if str(backend_path) not in sys.path:
    sys.path.insert(0, str(backend_path))


@pytest.fixture
def anyio_backend():
    """Use asyncio for async tests."""
    return "asyncio"
