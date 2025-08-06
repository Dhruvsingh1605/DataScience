from fastapi import FastAPI, Query
import pandas as pd
from query_parser import parse_query
from analyze import execute_query
import logging
from functools import lru_cache
import traceback

app = FastAPI()

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("app_debug.log"),
        logging.StreamHandler()
    ]
)

@lru_cache()
def load_data():
    logging.debug("Loading dataset from CSV...")
    try:
        df = pd.read_csv("vgsales_cleaned.csv")
        logging.debug(f"Loaded dataset with shape: {df.shape}")
        return df
    except Exception:
        logging.error("Failed to load dataset.")
        logging.error(traceback.format_exc())
        raise

@app.get("/ask")
def ask_query(q: str = Query(..., description="User natural language question")):
    print(f"📥 Received query: {q}")

    try:
        parsed = parse_query(q)
        print(f"🧠 Parsed query from LLM: {parsed}")
    except Exception as e:
        print("🚨 Unexpected error during parsing.")
        print(traceback.format_exc())
        return {"error": "Unexpected error during parsing."}

    if 'error' in parsed:
        print(f"❌ Query parsing failed: {parsed['error']}")
        return {
            "error": "Could not understand your query.",
            "details": parsed.get("details"),
            "raw": parsed.get("raw")
        }

    try:
        df = load_data()
    except Exception:
        return {"error": "Could not load dataset."}

    try:
        result = execute_query(df, parsed)
        print(f"✅ Query executed successfully. Result length: {len(result) if isinstance(result, list) else 'N/A'}")
        return result
    except Exception as e:
        print("🚨 Exception during query execution.")
        print(traceback.format_exc())
        return {"error": "Failed to execute your query."}
