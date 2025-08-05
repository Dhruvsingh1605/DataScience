import json
import google.generativeai as genai
from typing import Dict
from pydantic import BaseModel, ValidationError

with open("column_metadata.json") as f:
    schema_metadata = json.load(f)

class ParsedQuery(BaseModel):
    action: str
    target: str
    filters: list
    group_by: str = None
    sort: dict = None
    top_k: int = None

genai.configure(api_key="AIzaSyDkc8vs8itetqZ-44QYvaF7maoMDo9ajtE")

def build_prompt(user_input: str) -> str:
    return f"""
You are a smart tabular data query planner.
Here is the schema of the dataset:

{json.dumps(schema_metadata, indent=2)}

Now convert this user question into a JSON structured query:

User Question: "{user_input}"

Respond ONLY with a JSON object with the following keys:
- action (e.g., "sum", "count", "max", etc.)
- target (e.g., "Global_Sales", "Year", etc.)
- filters (list of {{column, op, value}})
- group_by (optional)
- sort (optional): {{column: "asc"/"desc"}}
- top_k (optional): number

No markdown, no explanations, no comments, no triple backticks.
"""

def sanitize_json(raw_text: str) -> str:
    raw_text = raw_text.strip()
    if raw_text.startswith("```json"):
        raw_text = raw_text.replace("```json", "").replace("```", "").strip()
    if "```" in raw_text:
        raw_text = raw_text.replace("```", "")
    return raw_text

def parse_query(user_input: str) -> Dict:
    prompt = build_prompt(user_input)

    try:
        model = genai.GenerativeModel(model_name="models/gemini-1.5-flash-latest")
        response = model.generate_content(prompt)
        raw = sanitize_json(response.text)

        print("[DEBUG] Gemini raw response:\n", raw)

        parsed_dict = json.loads(raw)
        structured = ParsedQuery(**parsed_dict).dict()

        return structured

    except (json.JSONDecodeError, ValidationError) as ve:
        print("[ERROR] Failed to parse structured query.")
        print("[DEBUG] Raw output:\n", raw)
        return {"error": f"Could not parse structured query: {str(ve)}"}

    except Exception as e:
        print("[ERROR] Gemini API call failed.")
        return {"error": f"Gemini error: {str(e)}"}
