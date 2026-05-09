import traceback
from app.main import visualization_service

svc = visualization_service

product_id = "redmi_note_13_5g"
competitor_product_id = "realme_narzo_70x_5g"

selected_name = svc._product_display_name(product_id)
competitor_name = svc._product_display_name(competitor_product_id)

print("selected_name:", selected_name)
print("competitor_name:", competitor_name)
print("should_use_spec_gemini:", svc._should_use_spec_gemini())
print("llm_status:", svc.llm_service.status())

try:
    prompt = svc._build_spec_prompt(
        selected_product_id=product_id,
        selected_product_name=selected_name,
        competitor_product_id=competitor_product_id,
        competitor_product_name=competitor_name,
    )
    print("prompt_length:", len(prompt))

    raw = svc.llm_service._call_gemini(prompt)
    print("raw Gemini response preview:")
    print(raw[:1000])

    parsed = svc._extract_json(raw)
    print("parsed keys:", parsed.keys())
    print("rows:", len(parsed.get("rows", [])))
    print("first row:", parsed.get("rows", [None])[0])

except Exception as e:
    print("ERROR TYPE:", type(e).__name__)
    print("ERROR:", str(e))
    traceback.print_exc()
