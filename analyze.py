import pandas as pd
import logging

logger = logging.getLogger(__name__)

def execute_query(df: pd.DataFrame, parsed_query: dict):
    if "error" in parsed_query:
        logger.warning("⚠️ Invalid parsed query structure: contains 'error' key.")
        return {"error": "Invalid query structure."}

    df_filtered = df.copy()
    logger.debug(f"🔍 Initial dataframe shape: {df.shape}")

    filters = parsed_query.get("filters", [])
    for f in filters:
        col, op, val = f["column"], f["op"], f["value"]
        op = op.lower().strip()

        logger.debug(f"🧪 Applying filter: {col} {op} {val}")
        try:
            if op in ["=", "==", "eq", "lookup"]:
                df_filtered = df_filtered[df_filtered[col] == val]
            elif op == "!=" or op == "ne":
                df_filtered = df_filtered[df_filtered[col] != val]
            elif op == ">" or op == "gt":
                df_filtered = df_filtered[df_filtered[col] > val]
            elif op == "<" or op == "lt":
                df_filtered = df_filtered[df_filtered[col] < val]
            elif op == ">=" or op == "gte":
                df_filtered = df_filtered[df_filtered[col] >= val]
            elif op == "<=" or op == "lte":
                df_filtered = df_filtered[df_filtered[col] <= val]
            elif op == "any":
                if isinstance(val, list):
                    df_filtered = df_filtered[df_filtered[col].isin(val)]
                else:
                    logger.warning(f"⚠️ 'any' operator requires a list value.")
                    return {"error": "'any' operator requires a list."}
            else:
                logger.warning(f"⚠️ Unsupported filter operation: {op}")
                return {"error": f"Unsupported filter operation: {op}"}
        except Exception as e:
            logger.error(f"❌ Error applying filter {f}: {str(e)}")
            return {"error": f"Invalid filter: {f}"}

    logger.debug(f"✅ Filtered dataframe shape: {df_filtered.shape}")

    group_col = parsed_query.get("group_by")
    target_col = parsed_query.get("target")
    action = parsed_query.get("action", "").lower().strip()

    if group_col:
        logger.debug(f"🔗 Grouping by '{group_col}', applying action '{action}' on '{target_col}'")
        try:
            df_grouped = df_filtered.groupby(group_col)

            actions_map = {
                "sum": df_grouped[target_col].sum,
                "count": df_grouped[target_col].count,
                "max": df_grouped[target_col].max,
                "min": df_grouped[target_col].min,
                "avg": df_grouped[target_col].mean,
                "median": df_grouped[target_col].median,
                "std": df_grouped[target_col].std,
                "var": df_grouped[target_col].var,
                "first": df_grouped[target_col].first,
                "last": df_grouped[target_col].last,
                "distinct": lambda: df_grouped[target_col].apply(lambda x: list(x.dropna().unique())),
                "count_distinct": df_grouped[target_col].nunique,
                "sum_distinct": lambda: df_grouped[target_col].apply(lambda x: x.dropna().unique().sum()),
                "list": lambda: df_grouped[target_col].apply(lambda x: list(x.dropna()))
            }

            if action in actions_map:
                result = actions_map[action]()
            elif action in ["eq", "lookup"]:
                lookup_col = parsed_query.get("lookup_col")
                if not lookup_col:
                    return {"error": "'lookup_col' must be specified for lookup action."}
                result = df_filtered[[target_col, lookup_col]].dropna().drop_duplicates()
                result = result.groupby(target_col)[lookup_col].apply(list).reset_index()
                return result.to_dict(orient="records")
            else:
                return {"error": f"Unsupported group-by action: {action}"}
        except Exception as e:
            logger.exception("🚨 Exception during group-by aggregation")
            return {"error": f"Failed during group-by aggregation: {str(e)}"}

        if parsed_query.get("sort"):
            try:
                sort_info = parsed_query["sort"]
                sort_column = sort_info.get("column", target_col)
                ascending = sort_info.get("order", "desc").lower() == "asc"
                result = result.sort_values(ascending=ascending, by=sort_column if hasattr(result, 'columns') else None)
            except Exception as e:
                logger.warning(f"⚠️ Sorting failed: {str(e)} — skipping sort.")

        if parsed_query.get("top_k"):
            try:
                k = int(parsed_query["top_k"])
                result = result.head(k)
            except Exception as e:
                logger.warning(f"⚠️ Top-k failed: {str(e)} — skipping top_k.")

        return result.reset_index().to_dict(orient="records")

    if not target_col:
        return {"error": "Missing 'target' column for non-grouped operation."}

    try:
        logger.debug(f"📈 Performing '{action}' on column '{target_col}' without grouping")
        if action == "count" and not target_col:
            logger.debug("📊 Counting total matching rows (no target_col)")
            return {"count": len(df_filtered)}
        if action == "sum":
            value = df_filtered[target_col].sum()
        elif action == "max":
            max_value = df_filtered[target_col].max()
            top_rows = df_filtered[df_filtered[target_col] == max_value]
            return top_rows[["Name", "Platform", "Year", target_col]].to_dict(orient="records")
        elif action == "min":
            value = df_filtered[target_col].min()
        elif action == "avg":
            value = df_filtered[target_col].mean()
        elif action == "count":
            value = df_filtered[target_col].count()
        elif action == "distinct":
            value = list(df_filtered[target_col].dropna().unique())
        elif action == "list":
            value = list(df_filtered[target_col].dropna())
        elif action == "filter" and not target_col and not group_col:
                result_df = df_filtered[["Name", "Platform", "Year", "Genre", "Publisher", "Global_Sales"]]
                result_df = result_df.sort_values("Global_Sales", ascending=False)
                result_df = result_df.head(10) 

                return result_df.to_dict(orient="records")
        else:
            return {"error": f"Unsupported non-grouped action: {action}"}
        return {target_col: value}
    except Exception as e:
        logger.exception(f"🚨 Exception during aggregation on column '{target_col}'")
        return {"error": f"Failed during aggregation: {str(e)}"}
