from pathlib import Path
import re

path = Path("src/App.tsx")
text = path.read_text(encoding="utf-8")

match = re.search(r'import\s*\{([^}]+)\}\s*from\s*["\']react["\'];', text)

if not match:
    raise SystemExit("React named import not found.")

imports = [item.strip() for item in match.group(1).split(",") if item.strip()]

if "useMemo" not in imports:
    imports.append("useMemo")

new_import = "import { " + ", ".join(imports) + ' } from "react";'
text = text[:match.start()] + new_import + text[match.end():]

path.write_text(text, encoding="utf-8")
print("Fixed React import: useMemo added.")
