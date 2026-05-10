from pathlib import Path
import os
import sys

ROOT = Path(__file__).resolve().parent
BACKEND = ROOT / "backend"

# Match local development behavior:
# locally we run uvicorn from irip_mvp_starter/backend,
# so relative paths like data/irip_mvp.db resolve correctly.
os.chdir(BACKEND)

if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from app.main import app  # noqa: E402
