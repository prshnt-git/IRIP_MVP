from pathlib import Path
import os
import sys

ROOT = Path(__file__).resolve().parent
BACKEND = ROOT / "backend"

os.chdir(ROOT)

if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from app.main import app  # noqa: E402
