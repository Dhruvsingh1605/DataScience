import json
import requests
from typing import Dict, Optional, List, Union, Any
from pydantic import BaseModel, field_validator, ValidationError
import logging
import os

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama3-8b-8192"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("QueryParser")

with open("column_metadata.json") as f:
    schema_metadata = json.load(f)

class ParsedQuery(BaseModel):
    action: str
    target: Optional[str] = None
    filters: List[Dict[str, Any]] = []
    group_by: Optional[Union[str, List[str]]] = None
    sort: Optional[Dict[str, str]] = None
    top_k: Optional[int] = None

    @field_validator("group_by")
    def normalize_group_by(cls, v):
        if isinstance(v, list):
            return v[0] if v else None
        return v

def build_prompt(user_input: str) -> str:
    return f"""

You are a smart tabular data query planner and the best data analyst working for a very big tech company.
Here is the schema of the dataset:

{json.dumps(schema_metadata, indent=2)}

First the context and intent of the user question and background information about the dataset. Now convert this user question into a JSON structured query
Even if the user question is not clear, try to extract the most relevant information and convert it into a structured query.
Even if the user uses vague terms or poor english, try to extract the most relevant information and convert it into a structured query.

User Question: "{user_input}"

Respond ONLY with a JSON object with the following keys:
- action (e.g., "sum", "count", "max", "min", "avg")
- target (e.g., "Global_Sales", "Year", etc.)
- filters (a list of objects in the form: 
  {{"column": ..., "op": ..., "value": ...}} 
  where "op" is one of:
  "=", "==", "eq", "!=", "ne", ">", "gt", "<", "lt", ">=", "gte", "<=", "lte", "any"
)
- group_by (optional): string or list of strings
- sort (optional): {{"column": ..., "order": "asc" or "desc"}}
- top_k (optional): integer

Respond with a JSON object like this:

{{
    "action": "sum",
    "target": "Global_Sales",
    "filters": [
        {{"column": "Platform", "op": "=", "value": "PS4"}},
        {{"column": "Year", "op": ">=", "value": 2010}}
    ],
    "group_by": ["Genre"],
    "sort": {{"column": "Global_Sales", "order": "desc"}},
    "top_k": 5
}}

If you cannot extract a particular field, you can omit it. But try to extract target for sure after understanding the user question.
No markdown, no explanations, no comments, no triple backticks.
""".strip()


def sanitize_json(raw_text: str) -> str:
    raw_text = raw_text.strip()
    if raw_text.startswith("```json"):
        raw_text = raw_text.replace("```json", "").replace("```", "").strip()
    elif raw_text.startswith("```"):
        raw_text = raw_text.replace("```", "").strip()
    return raw_text

def parse_query(user_input: str) -> Dict[str, Any]:
    logger.info(f"📥 Received query: {user_input}")
    prompt = build_prompt(user_input)

    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {
                "role": "system",
                "content": "You convert natural language questions into structured JSON queries based on the dataset schema."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.2,
        "max_tokens": 512,
    }

    try:
        logger.debug("Sending request to Groq API...")
        response = requests.post(GROQ_API_URL, headers={
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json"
        }, json=payload)

        response.raise_for_status()
        raw_output = response.json()["choices"][0]["message"]["content"]
        logger.debug("[DEBUG] Groq LLaMA response:\n%s", raw_output)

        cleaned = sanitize_json(raw_output)
        parsed = json.loads(cleaned)
        try:
            validated = ParsedQuery(**parsed)
            result = validated.model_dump()
            logger.info(f"🧠 Final parsed result: {result}")   
            return result

        except ValidationError as ve:
            logger.warning("Validation failed on parsed output: %s", ve)
            return {
                "error": "Validation failed on parsed query.",
                "raw": parsed,
                "details": ve.errors()
            }

    except requests.RequestException as e:
        logger.error("❌ Network/API error: %s", str(e))
        return {"error": f"Groq API error: {str(e)}"}
    except json.JSONDecodeError as e:
        logger.error("❌ JSON decode error: %s", str(e))
        return {"error": f"Failed to decode Groq output: {str(e)}"}

