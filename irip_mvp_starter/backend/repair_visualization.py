from pathlib import Path
import re

path = Path("app/services/visualization_service.py")
text = path.read_text(encoding="utf-8")

pattern = r'(?ms)^[ \t]*def _build_spec_prompt\(.*?(?=^[ \t]*def _validate_spec_table\()'

replacement = r'''    def _build_spec_prompt(
        self,
        selected_product_id: str,
        selected_product_name: str,
        competitor_product_id: str,
        competitor_product_name: str,
    ) -> str:
        fields = [
            ("Commercial", "Current price"),
            ("Display", "Display size"),
            ("Display", "Display type"),
            ("Display", "Refresh rate"),
            ("Performance", "Chipset"),
            ("Performance", "RAM / Storage"),
            ("Battery", "Battery capacity"),
            ("Battery", "Charging wattage"),
            ("Camera", "Rear camera"),
            ("Camera", "Front camera"),
            ("Software", "Android / UI"),
            ("Network", "5G support"),
            ("Design", "Weight"),
            ("Design", "Thickness"),
        ]

        payload = {
            "selected_product": selected_product_name,
            "competitor_product": competitor_product_name,
            "required_fields": [{"category": category, "field": field} for category, field in fields],
        }

        return f"""
Return ONLY valid JSON.

Create a compact smartphone specification comparison table.

Products:
Selected product: {selected_product_name}
Competitor: {competitor_product_name}

Rules:
- Use your model knowledge.
- If unsure, use "Unknown".
- Keep values short.
- No markdown.
- No explanation outside JSON.
- confidence: verified | likely | unknown
- source_status: model_knowledge | needs_source | unknown
- winner: selected_product | competitor | tie | unknown | not_applicable

JSON schema:
{{
  "selected_product_name": "{selected_product_name}",
  "competitor_product_name": "{competitor_product_name}",
  "source": "gemini",
  "confidence_note": "AI-generated specs. Verify with official sources before business use.",
  "rows": [
    {{
      "category": "Battery",
      "field": "Battery capacity",
      "selected_product_value": "5000 mAh",
      "competitor_value": "5000 mAh",
      "winner": "tie",
      "confidence": "likely",
      "source_status": "model_knowledge",
      "why_it_matters": "Useful for product comparison."
    }}
  ],
  "unknown_fields": []
}}

Required rows:
{json.dumps(payload["required_fields"], ensure_ascii=False)}
""".strip()


'''

new_text, count = re.subn(pattern, replacement, text, count=1)

if count != 1:
    print("Could not patch automatically. Showing nearby method names:")
    for match in re.finditer(r'^[ \t]*def _.*', text, flags=re.MULTILINE):
        print(match.group(0))
    raise SystemExit(1)

path.write_text(new_text, encoding="utf-8")
print("Patched _build_spec_prompt successfully.")
