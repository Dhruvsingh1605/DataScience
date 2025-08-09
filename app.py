# import sqlite3
# import requests
# import matplotlib.pyplot as plt
# from datetime import datetime

# GROQ_API_KEY = "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"
# GROQ_MODEL = "llama3-8b-8192"
# DATABASE_PATH = "games.db"
# TABLE_NAME = "game_sales"

# chat_history = []

# def add_to_history(question, sql, result_preview, explanation=None):
#     entry = {
#         "timestamp": datetime.now().isoformat(),
#         "question": question,
#         "sql": sql,
#         "result_preview": result_preview[:5] if result_preview else [],
#         "explanation": explanation or "",
#     }
#     chat_history.append(entry)

# def print_history():
#     print("\n🕓 CHAT HISTORY (latest 5):")
#     for i, h in enumerate(chat_history[-5:], 1):
#         print(f"{i}. [{h['timestamp']}]")
#         print(f"   Q: {h['question']}")
#         print(f"   SQL: {h['sql']}")
#         if h['result_preview']:
#             print(f"   Top result: {h['result_preview'][0]}")
#         if h['explanation']:
#             print(f"   Insight: {h['explanation'][:80]}...")
#         print("-" * 40)

# def get_schema():
#     conn = sqlite3.connect(DATABASE_PATH)
#     cursor = conn.cursor()
#     cursor.execute(f"PRAGMA table_info({TABLE_NAME});")
#     columns = cursor.fetchall()
#     conn.close()
#     return {col[1]: col[2] for col in columns}

# def call_groq(messages):
#     headers = {
#         "Authorization": f"Bearer {GROQ_API_KEY}",
#         "Content-Type": "application/json",
#     }
#     payload = {
#         "model": GROQ_MODEL,
#         "messages": messages
#     }
#     response = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload)

#     if response.status_code != 200:
#         raise Exception(f"GROQ API Error {response.status_code}: {response.text}")

#     return response.json()["choices"][0]["message"]["content"].strip()

# def run_sql_query(sql):
#     try:
#         conn = sqlite3.connect(DATABASE_PATH)
#         cursor = conn.cursor()
#         cursor.execute(sql)
#         rows = cursor.fetchall()
#         headers = [desc[0] for desc in cursor.description]
#         conn.close()
#         return headers, rows
#     except Exception as e:
#         return None, str(e)

# def sql_generation_prompt(user_question, schema_dict):
#     schema_str = "\n".join([f"{col} ({dtype})" for col, dtype in schema_dict.items()])
#     return [
#         {
#             "role": "user",
#             "content": f"""
# You are an expert data analyst. You must understand the user's question even if the English is not perfect.
# The database has the following schema:
# Rank	Name	Platform	Year	Genre	Publisher	NA_Sales	EU_Sales	JP_Sales	Other_Sales	Global_Sales
# So, think accordingly to the schema and generate a syntactically valid SQLite query that answers the user's question.


# The table name is: {TABLE_NAME}
# Columns with types:
# {schema_str}

# Note:
# - Sales columns are likely: 'EU_Sales', 'JP_Sales', 'NA_Sales', 'Global_Sales'.
# - Do NOT filter by 'Platform' to identify region-specific data. Use the appropriate *_Sales column.
# - Use correct column names in WHERE/ORDER BY clauses.
# - Always write syntactically valid SQLite.
# - Only output the SQL query. No explanations.

# User Question: "{user_question}"
# """
#         }
#     ]

# def explanation_prompt(user_question, sql_query, query_result):
#     rows_preview = "\n".join([str(row) for row in query_result[:5]]) if query_result else "No results"
#     return [
#         {
#             "role": "user",
#             "content": f"""
# A user asked: "{user_question}"

# The following SQL was used to answer the question:
# {sql_query}

# The top rows of the result are:
# {rows_preview}

# Now explain this result in natural language in the context of the user's question. Be helpful and clear.
# You are a Senior Strategy Consultant at McKinsey & Company with expertise in data-driven decision making and predictive modeling. You advise Fortune 500 CEOs.
# So answer like a McKinsey consultant would, using data-driven insights and strategic thinking.
# """
#         }
#     ]

# def followup_question_prompt(user_question):
#     return [
#         {
#             "role": "user",
#             "content": f"""
# A user asked: "{user_question}"

# Unfortunately, no matching data was found in the database.

# Generate 2-3 smart, high-level, data-driven follow-up questions that a Senior McKinsey consultant would ask to better understand the user's intent and provide guidance.
# These should help reframe or clarify the user's needs.
# Respond only with the questions (numbered).
# """
#         }
#     ]

# def user_wants_graph(question):
#     keywords = ["graph", "plot", "chart", "visual", "visualize", "show me a graph"]
#     return any(kw in question.lower() for kw in keywords)

# def classify_intent_prompt(user_question):
#     return [
#         {
#             "role": "user",
#             "content": f'''
# A user asked the following question:
# """
# {user_question}
# """

# Classify this into one of these intent categories:
# - just_sql: If the user is just asking a question that can be answered by a SQL query.
# - sql_then_plot: If the user also wants a graph/chart/visualization of the result.
# - sql_then_explanation: If the user seems to want an explanation or strategic insight.
# - sql_then_plot_and_explanation: If both visual and explanation are expected.

# Return only the intent name (e.g., sql_then_plot), nothing else.
# '''
#         }
#     ]

# def plot_query_result(headers, rows):
#     if not rows or not headers:
#         print("⚠️ Cannot plot: No data.")
#         return

#     try:
#         if len(headers) == 2:
#             x_vals = [str(row[0]) for row in rows]
#             y_vals = [float(row[1]) for row in rows]

#             plt.figure(figsize=(10, 5))
#             plt.bar(x_vals, y_vals)
#             plt.xlabel(headers[0])
#             plt.ylabel(headers[1])
#             plt.title(f"{headers[1]} by {headers[0]}")
#             plt.xticks(rotation=45)
#             plt.tight_layout()
#             plt.grid(axis='y', linestyle='--', alpha=0.5)
#             plt.show()

#         elif len(headers) == 1:
#             y_vals = [float(row[0]) for row in rows]
#             plt.plot(y_vals, marker='o')
#             plt.title(f"{headers[0]} over index")
#             plt.ylabel(headers[0])
#             plt.xlabel("Index")
#             plt.grid(True)
#             plt.tight_layout()
#             plt.show()
#         else:
#             print("⚠️ Cannot automatically plot more than 2 dimensions.")
#     except Exception as e:
#         print("❌ Failed to plot graph:", str(e))

# def sanitize_sql(raw_sql):
#     return raw_sql.strip().strip("`").replace("```sql", "").replace("```", "").strip()


# def main():
#     schema = get_schema()
#     print("📊 DATABASE SCHEMA:")
#     for col, dtype in schema.items():
#         print(f"- {col} ({dtype})")
#     print("=" * 60)

#     while True:
#         question = input("💬 USER QUESTION: ")
#         if question.strip().lower() in ("exit", "quit"):
#             print("👋 Exiting.")
#             break
#         if question.strip().lower() == "history":
#             print_history()
#             continue

#         intent = call_groq(classify_intent_prompt(question)).strip()
#         print(f"🔎 Intent classified as: {intent}")

#         sql_prompt = sql_generation_prompt(question, schema)
#         sql_query = sanitize_sql(call_groq(sql_prompt).strip())

#         print("🧾 GENERATED SQL:")
#         print(sql_query)

#         headers, result = run_sql_query(sql_query)
#         if headers is None:
#             print("❌ SQL EXECUTION FAILED:")
#             print(result)
#             continue

#         if not result:
#             print("⚠️ No data found for this query.")
#             print("🤖 The data you're asking for does not exist in the current database.")
#             consult = input("🧠 Would you like me to consult you instead? (yes/no): ").strip().lower()

#             if consult == "yes":
#                 followups = call_groq(followup_question_prompt(question))
#                 print("📋 To help you better, please answer the following questions:")
#                 answers = []
#                 for q in followups.split("\n"):
#                     if q.strip():
#                         ans = input(f"{q.strip()} ").strip()
#                         answers.append(f"{q.strip()} {ans}")
#                 enriched_question = question + "\n" + "\n".join(answers)
#                 explanation = call_groq(explanation_prompt(enriched_question, sql_query, result)).strip()
#                 print("🧠 STRATEGIC CONSULTATION:")
#                 print(explanation)
#                 add_to_history(question, sql_query, [], explanation)
#             else:
#                 print("❌ Consultation skipped.")
#             print("=" * 60)
#             continue

#         print("✅ SQL RESULT:")
#         print("\t".join(headers))
#         for row in result:
#             print("\t".join(str(x) for x in row))
#         print("-" * 50)

#         if intent in ("sql_then_plot", "sql_then_plot_and_explanation"):
#             print("📈 Rendering graph...")
#             plot_query_result(headers, result)

#         explanation = ""
#         if intent in ("sql_then_explanation", "sql_then_plot_and_explanation"):
#             explanation = call_groq(explanation_prompt(question, sql_query, result)).strip()
#             print("🗣️ EXPLANATION:")
#             print(explanation)
#         else:
#             ask_explanation = input("💼 Do you want explanation or consultancy? (yes/no): ").strip().lower()
#             if ask_explanation == "yes":
#                 explanation = call_groq(explanation_prompt(question, sql_query, result)).strip()
#                 print("🗣️ EXPLANATION:")
#                 print(explanation)
#             else:
#                 print("ℹ️ Skipping explanation/consultancy.")

#         add_to_history(question, sql_query, result, explanation)
#         print("=" * 60)


# if __name__ == "__main__":
#     main()




import sqlite3
import requests
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import streamlit as st
import pandas as pd
from datetime import datetime
import io

sns.set_style("whitegrid")
plt.style.use('seaborn-v0_8')

GROQ_API_KEY = "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"
GROQ_MODEL = "llama3-8b-8192"
DATABASE_PATH = "games.db"
TABLE_NAME = "game_sales"

if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'df' not in st.session_state:
    st.session_state.df = None
if 'schema' not in st.session_state:
    st.session_state.schema = {}
if 'show_debug' not in st.session_state:
    st.session_state.show_debug = False

def load_data_from_sqlite():
    """Load data from SQLite into pandas DataFrame"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading data from SQLite: {e}")
        return None

def add_to_chat_history(message_type, content, sql_query=None, chart_type=None):
    """Add message to chat history"""
    entry = {
        "timestamp": datetime.now().isoformat(),
        "type": message_type,  
        "content": content,
        "sql_query": sql_query,
        "chart_type": chart_type
    }
    st.session_state.chat_history.append(entry)

def get_schema():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({TABLE_NAME});")
    columns = cursor.fetchall()
    conn.close()
    return {col[1]: col[2] for col in columns}

def call_groq(messages):
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": GROQ_MODEL,
        "messages": messages
    }
    response = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload)

    if response.status_code != 200:
        raise Exception(f"GROQ API Error {response.status_code}: {response.text}")

    return response.json()["choices"][0]["message"]["content"].strip()

def run_sql_query_on_dataframe(sql, df):
    """Execute SQL query on pandas DataFrame using pandasql or native pandas operations"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        conn.close()
        
        if rows:
            result_df = pd.DataFrame(rows, columns=headers)
            return headers, rows, result_df
        else:
            return headers, [], pd.DataFrame()
    except Exception as e:
        return None, str(e), None

def sql_generation_prompt(user_question, schema_dict):
    schema_str = "\n".join([f"{col} ({dtype})" for col, dtype in schema_dict.items()])
    return [
        {
            "role": "user",
            "content": f"""
You are an expert data analyst. You must understand the user's question even if the English is not perfect.
The database has the following schema:
Rank	Name	Platform	Year	Genre	Publisher	NA_Sales	EU_Sales	JP_Sales	Other_Sales	Global_Sales
So, think accordingly to the schema and generate a syntactically valid SQLite query that answers the user's question.

The table name is: {TABLE_NAME}
Columns with types:
{schema_str}

Note:
- Sales columns are likely: 'EU_Sales', 'JP_Sales', 'NA_Sales', 'Global_Sales'.
- Do NOT filter by 'Platform' to identify region-specific data. Use the appropriate *_Sales column.
- Use correct column names in WHERE/ORDER BY clauses.
- Always write syntactically valid SQLite.
- Only output the SQL query. No explanations.

User Question: "{user_question}"
"""
        }
    ]

def chart_type_selection_prompt(user_question, headers, sample_data):
    """Determine what type of chart/visualization would be best for the data"""
    sample_str = "\n".join([str(row) for row in sample_data[:3]]) if sample_data else "No data"
    
    return [
        {
            "role": "user",
            "content": f"""
A user asked: "{user_question}"

The query returned data with these columns: {headers}
Sample data:
{sample_str}

Based on the question and data structure, what type of visualization would be most appropriate?

Available chart types:
- bar: For categorical comparisons
- line: For trends over time or continuous data
- pie: For showing proportions/percentages
- scatter: For relationships between two continuous variables
- heatmap: For correlation matrices or 2D data intensity
- histogram: For distribution of a single variable
- box: For showing distribution and outliers
- area: For showing trends with filled areas
- donut: For proportions (alternative to pie)
- violin: For distribution shape and statistics

Consider:
1. If the user specifically mentioned a chart type (heatmap, pie chart, etc.), use that
2. If showing sales over time/years, use line chart
3. If comparing categories/platforms/genres, use bar chart
4. If showing proportions/market share, use pie chart
5. If showing correlation between variables, use scatter or heatmap
6. For single variable distribution, use histogram

Return only the chart type name (e.g., "bar", "line", "heatmap", etc.), nothing else.
"""
        }
    ]

def explanation_prompt(user_question, sql_query, query_result):
    rows_preview = "\n".join([str(row) for row in query_result[:5]]) if query_result else "No results"
    return [
        {
            "role": "user",
            "content": f"""
A user asked: "{user_question}"

The following SQL was used to answer the question:
{sql_query}

The top rows of the result are:
{rows_preview}

Now explain this result in natural language in the context of the user's question. Be helpful and clear.
You are a Senior Strategy Consultant at McKinsey & Company with expertise in data-driven decision making and predictive modeling. You advise Fortune 500 CEOs.
So answer like a McKinsey consultant would, using data-driven insights and strategic thinking.
Don't tell the user who you are and what is your job role.
"""
        }
    ]

def get_plot_columns(result_df, chart_type):
    """Identify appropriate x and y columns based on data types and chart type"""
    numeric_cols = result_df.select_dtypes(include=[np.number]).columns.tolist()
    non_numeric_cols = result_df.select_dtypes(exclude=[np.number]).columns.tolist()
    
    if chart_type in ['bar', 'line', 'area']:
        x_col = non_numeric_cols[0] if non_numeric_cols else result_df.columns[0]
        y_col = numeric_cols[0] if numeric_cols else result_df.columns[1]
        return x_col, y_col
    
    elif chart_type in ['pie', 'donut']:
        label_col = non_numeric_cols[0] if non_numeric_cols else result_df.columns[0]
        value_col = numeric_cols[0] if numeric_cols else result_df.columns[1]
        return label_col, value_col
    
    elif chart_type == 'scatter':
        if len(numeric_cols) >= 2:
            return numeric_cols[0], numeric_cols[1]
        return None, None
    
    elif chart_type in ['histogram', 'box']:
        return None, numeric_cols[0] if numeric_cols else None
    
    return result_df.columns[0], result_df.columns[1]

def create_advanced_chart(chart_type, result_df, user_question):
    """Create charts using proper data type handling"""
    if result_df is None or result_df.empty:
        st.warning("⚠️ Cannot plot: No data.")
        return

    try:
        fig, ax = plt.subplots(figsize=(12, 8))
        
        x_col, y_col = get_plot_columns(result_df, chart_type)
        
        if chart_type == "bar":
            if not x_col or not y_col:
                raise ValueError("Missing required columns for bar chart")
                
            plot_df = result_df.sort_values(y_col, ascending=False).head(15)
            
            bars = ax.bar(plot_df[x_col].astype(str), plot_df[y_col], 
                          color='skyblue', alpha=0.8)
            ax.set_xlabel(x_col)
            ax.set_ylabel(y_col)
            ax.set_title(f"{y_col} by {x_col}")
            plt.xticks(rotation=45, ha='right')
            
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2, height + 0.01*height,
                        f'{height:.1f}', ha='center', va='bottom')
                
        elif chart_type == "line":
            if not x_col or not y_col:
                raise ValueError("Missing required columns for line chart")
                
            if np.issubdtype(result_df[x_col].dtype, np.number) or result_df[x_col].dtype == 'object':
                try:
                    result_df[x_col] = pd.to_datetime(result_df[x_col])
                except:
                    pass
                    
            plot_df = result_df.sort_values(x_col)
            ax.plot(plot_df[x_col].astype(str), plot_df[y_col], 
                    marker='o', linewidth=2, markersize=6, color='darkblue')
            ax.set_xlabel(x_col)
            ax.set_ylabel(y_col)
            ax.set_title(f"{y_col} Trend by {x_col}")
            plt.xticks(rotation=45, ha='right')
            ax.fill_between(plot_df[x_col].astype(str), plot_df[y_col], alpha=0.3, color='lightblue')
            
        elif chart_type in ["pie", "donut"]:
            if not x_col or not y_col:
                raise ValueError("Missing required columns for pie chart")
                
            plot_df = result_df.sort_values(y_col, ascending=False).head(8)
            labels = plot_df[x_col].astype(str)
            sizes = plot_df[y_col]
            
            if chart_type == "pie":
                colors = plt.cm.Set3(np.linspace(0, 1, len(labels)))
                wedges, texts, autotexts = ax.pie(sizes, labels=labels, autopct='%1.1f%%', 
                                                 colors=colors, startangle=90)
                ax.set_title(f"Distribution of {y_col} by {x_col}")
                
            else:  
                colors = plt.cm.Pastel1(np.linspace(0, 1, len(labels)))
                wedges, texts, autotexts = ax.pie(sizes, labels=labels, autopct='%1.1f%%',
                                                 colors=colors, startangle=90, pctdistance=0.85)
                centre_circle = plt.Circle((0,0), 0.70, fc='white')
                ax.add_artist(centre_circle)
                ax.set_title(f"Distribution of {y_col} by {x_col}")
                
        elif chart_type == "scatter":
            if not x_col or not y_col:
                raise ValueError("Missing required numeric columns for scatter plot")
                
            ax.scatter(result_df[x_col], result_df[y_col], alpha=0.6, s=60, color='coral')
            ax.set_xlabel(x_col)
            ax.set_ylabel(y_col)
            ax.set_title(f"{y_col} vs {x_col}")
            
        elif chart_type == "heatmap":
            numeric_cols = result_df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) < 2:
                raise ValueError("Need at least 2 numeric columns for heatmap")
                
            corr_matrix = result_df[numeric_cols].corr()
            sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0, 
                       square=True, ax=ax, cbar_kws={"shrink": .8})
            ax.set_title("Correlation Heatmap")
            
        elif chart_type == "histogram":
            if not y_col:
                raise ValueError("Missing numeric column for histogram")
                
            values = result_df[y_col].dropna()
            ax.hist(values, bins=min(20, len(values)//2 + 1), 
                    alpha=0.7, color='lightgreen', edgecolor='black')
            ax.set_xlabel(y_col)
            ax.set_ylabel("Frequency")
            ax.set_title(f"Distribution of {y_col}")
            
        elif chart_type == "box":
            if not y_col:
                raise ValueError("Missing numeric column for box plot")
                
            numeric_data = [result_df[y_col].dropna()]
            ax.boxplot(numeric_data, labels=[y_col])
            ax.set_title("Box Plot Distribution")
            plt.xticks(rotation=45, ha='right')
            
        elif chart_type == "area":
            if not x_col or not y_col:
                raise ValueError("Missing required columns for area chart")
                
            plot_df = result_df.sort_values(x_col)
            x_vals = range(len(plot_df))
            y_vals = plot_df[y_col]
            
            ax.fill_between(x_vals, y_vals, alpha=0.4, color='lightcoral')
            ax.plot(x_vals, y_vals, color='darkred', linewidth=2)
            ax.set_xlabel("Data Points")
            ax.set_ylabel(y_col)
            ax.set_title(f"{y_col} Area Chart")
            
        else:
            return create_advanced_chart("bar", result_df, user_question)
        
        plt.tight_layout()
        st.pyplot(fig)
        plt.close()
        
    except Exception as e:
        st.error(f"❌ Failed to create {chart_type} chart: {str(e)}")
        st.info("Attempting fallback to bar chart...")
        try:
            if not result_df.empty:
                create_advanced_chart("bar", result_df, user_question)
            else:
                st.warning("No data available for fallback chart")
        except Exception as fallback_error:
            st.error(f"❌ Fallback chart failed: {str(fallback_error)}")

def sanitize_sql(raw_sql):
    return raw_sql.strip().strip("`").replace("```sql", "").replace("```", "").strip()

def display_chat_message(message_type, content, sql_query=None, chart_type=None):
    """Display a chat message with appropriate styling"""
    if message_type == "user":
        with st.chat_message("user"):
            st.write(content)
    elif message_type == "assistant":
        with st.chat_message("assistant"):
            st.write(content)
            if sql_query and st.session_state.show_debug:
                with st.expander("🐛 Debug - SQL Query"):
                    st.code(sql_query, language='sql')
            if chart_type:
                st.info(f"📊 Generated {chart_type.title()} Chart")
    elif message_type == "system":
        with st.chat_message("assistant"):
            st.info(content)

def main():
    st.set_page_config(
        page_title="🎮 Game Sales Chat Assistant",
        page_icon="🎮",
        layout="wide"
    )
    
    st.title("🎮 Game Sales Chat Assistant")
    st.markdown("Ask me anything about game sales data! I can generate SQL queries, create visualizations, and provide strategic insights.")
    
    if st.session_state.df is None:
        with st.spinner("Loading data from SQLite database..."):
            st.session_state.df = load_data_from_sqlite()
            st.session_state.schema = get_schema()
    
    with st.sidebar:
        st.header("🛠️ Controls")
        
        debug_enabled = st.toggle("🐛 Debug Mode", value=st.session_state.show_debug)
        if debug_enabled != st.session_state.show_debug:
            st.session_state.show_debug = debug_enabled
            st.rerun()
        
        st.markdown("---")
        
        st.header("📊 Database Schema")
        if st.session_state.schema:
            for col, dtype in st.session_state.schema.items():
                st.text(f"• {col} ({dtype})")
        
        st.markdown("---")
        
        if st.button("🗑️ Clear Chat History"):
            st.session_state.chat_history = []
            st.rerun()
        
        if st.button("📊 Show Preview"):
            if st.session_state.df is not None:
                st.subheader("Data Preview")
                st.dataframe(st.session_state.df.head(), use_container_width=True)
    
    st.markdown("---")
    
    for message in st.session_state.chat_history:
        display_chat_message(message["type"], message["content"], 
                           message.get("sql_query"), message.get("chart_type"))
    
    if prompt := st.chat_input("Ask me about game sales data..."):
        add_to_chat_history("user", prompt)
        display_chat_message("user", prompt)
        
        with st.chat_message("assistant"):
            with st.spinner("🤔 Thinking..."):
                try:
                    sql_prompt = sql_generation_prompt(prompt, st.session_state.schema)
                    sql_query = sanitize_sql(call_groq(sql_prompt).strip())
                    
                    headers, result, result_df = run_sql_query_on_dataframe(sql_query, st.session_state.df)
                    
                    if headers is None:
                        error_msg = f"❌ SQL Execution Failed: {result}"
                        st.error(error_msg)
                        add_to_chat_history("assistant", error_msg, sql_query)
                        return
                    
                    if not result:
                        no_data_msg = "⚠️ No data found for your query. Try rephrasing your question or asking about different aspects of the game sales data."
                        st.warning(no_data_msg)
                        add_to_chat_history("assistant", no_data_msg, sql_query)
                        return
                    
                    st.success(f"✅ Found {len(result)} results!")
                    st.dataframe(result_df, use_container_width=True)
                    
                    wants_chart = any(word in prompt.lower() for word in 
                                    ['chart', 'graph', 'plot', 'visualiz', 'heatmap', 'pie', 'bar', 'line', 'show'])
                    
                    chart_type = None
                    if wants_chart and len(result) > 0:
                        chart_prompt = chart_type_selection_prompt(prompt, headers, result)
                        chart_type = call_groq(chart_prompt).strip().lower()
                        
                        st.subheader(f"📊 {chart_type.title()} Chart")
                        create_advanced_chart(chart_type, result_df, prompt)
                    
                    with st.spinner("Generating insights..."):
                        explanation = call_groq(explanation_prompt(prompt, sql_query, result)).strip()
                        
                        st.subheader("🧠 Strategic Insights")
                        st.markdown(explanation)
                    
                    response_msg = f"Found {len(result)} results. " + ("Generated visualization and " if chart_type else "") + "provided strategic insights."
                    add_to_chat_history("assistant", response_msg, sql_query, chart_type)
                    
                except Exception as e:
                    error_msg = f"❌ An error occurred: {str(e)}"
                    st.error(error_msg)
                    add_to_chat_history("assistant", error_msg)

if __name__ == "__main__":
    main()