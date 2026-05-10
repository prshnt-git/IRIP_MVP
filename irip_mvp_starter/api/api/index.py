from pathlib import Path
import os
import sys

# File location on local:
# irip_mvp_starter/api/api/index.py
#
# Project root:
# irip_mvp_starter
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BACKEND = PROJECT_ROOT / "backend"

os.chdir(PROJECT_ROOT)

if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from app.main import app  # noqa: E402
