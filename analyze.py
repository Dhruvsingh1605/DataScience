import pandas as pd

def execute_query(df: pd.DataFrame, parsed_query: dict):
    if "error" in parsed_query:
        return {"error": "Invalid query structure."}

    df_filtered = df.copy()

    for f in parsed_query.get("filters", []):
        col, op, val = f["column"], f["op"], f["value"]
        if op == "==":
            df_filtered = df_filtered[df_filtered[col] == val]
        elif op == ">":
            df_filtered = df_filtered[df_filtered[col] > val]
        elif op == "<":
            df_filtered = df_filtered[df_filtered[col] < val]
        # Extend with more ops...

    if parsed_query.get("group_by"):
        df_grouped = df_filtered.groupby(parsed_query["group_by"])[parsed_query["target"]]
        if parsed_query["action"] == "sum":
            result = df_grouped.sum()
        elif parsed_query["action"] == "count":
            result = df_grouped.count()
        elif parsed_query["action"] == "max":
            result = df_grouped.max()
        else:
            raise ValueError("Unsupported action")

        # Sorting
        if parsed_query.get("sort"):
            sort_key = parsed_query["sort"]["by"]
            ascending = parsed_query["sort"]["order"] == "asc"
            result = result.sort_values(ascending=ascending)

        # Top K
        if parsed_query.get("top_k"):
            result = result.head(parsed_query["top_k"])

        return result.to_dict()

    else:
        # No grouping
        if parsed_query["action"] == "sum":
            return {parsed_query["target"]: df_filtered[parsed_query["target"]].sum()}
        elif parsed_query["action"] == "max":
            return {parsed_query["target"]: df_filtered[parsed_query["target"]].max()}
        elif parsed_query["action"] == "count":
            return {parsed_query["target"]: df_filtered[parsed_query["target"]].count()}
        else:
            return {"error": "Unsupported action"}
