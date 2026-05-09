from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

SECRET_PATTERNS = [
    re.compile(r"AIzaSy[A-Za-z0-9_\-]{20,}"),
    re.compile(r"\bsk-[A-Za-z0-9_\-]{20,}"),
    re.compile(r"\bghp_[A-Za-z0-9_]{20,}"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}"),
    re.compile(r"^\s*GEMINI_API_KEY\s*=\s*(?!your_|fake|placeholder|$)[^\s#]+", re.I | re.M),
    re.compile(r"^\s*IRIP_PRODUCT_CATALOG_CSV_URL\s*=\s*https?://", re.I | re.M),
]

ALLOW_FILES = {
    "backend/.env.example",
    "frontend/.env.example",
}

def git_files(args: list[str]) -> list[str]:
    result = subprocess.run(
        args,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]

def main() -> int:
    files = set(git_files(["git", "ls-files"]))
    files.update(git_files(["git", "diff", "--cached", "--name-only"]))

    findings = []

    for rel in sorted(files):
        if rel in ALLOW_FILES:
            continue

        path = ROOT / rel
        if not path.exists() or not path.is_file():
            continue

        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        for pattern in SECRET_PATTERNS:
            for match in pattern.finditer(text):
                line_no = text[:match.start()].count("\n") + 1
                findings.append(f"{rel}:{line_no}: possible secret")

    if findings:
        print("Potential tracked/staged secrets found:")
        for item in findings:
            print("-", item)
        return 1

    print("Tracked/staged secret scan OK.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
