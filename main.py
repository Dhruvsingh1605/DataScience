from fastapi import FastAPI, Query
import pandas as pd
from query_parser import parse_query
from analyze import execute_query
import logging
from functools import lru_cache

app = FastAPI()
logging.basicConfig(level=logging.INFO)

@lru_cache()
def load_data():
    return pd.read_csv("vgsales_cleaned.csv")

@app.get("/ask")
def ask_query(q: str = Query(...)):
    logging.info(f"Received query: {q}")
    parsed = parse_query(q)
    logging.info(f"Parsed query: {parsed}")

    df = load_data()
    result = execute_query(df, parsed)
    return result
