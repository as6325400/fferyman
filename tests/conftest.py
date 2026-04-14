from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"
sys.path.insert(0, str(EXAMPLES_DIR))


@pytest.fixture
def logger():
    return logging.getLogger("fferyman.tests")
