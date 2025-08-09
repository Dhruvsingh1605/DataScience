import sqlite3
import requests
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import streamlit as st
import pandas as pd
from datetime import datetime
import io
import re
import logging
from typing import Optional, Tuple, Dict, List, Any
import time
from functools import wraps

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set plotting style
sns.set_style("whitegrid")
plt.style.use('seaborn-v0_8')

# Configuration
GROQ_API_KEY = "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"
GROQ_MODEL = "llama3-8b-8192"
DATABASE_PATH = "games.db"
TABLE_NAME = "game_sales"
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30

# Initialize session state
def initialize_session_state():
    """Initialize all session state variables"""
    defaults = {
        'chat_history': [],
        'df': None,
        'schema': {},
        'show_debug': False,
        'data_loaded': False,
        'error_count': 0
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

def retry_on_failure(max_retries: int = MAX_RETRIES):
    """Decorator for retrying failed operations"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Function {func.__name__} failed after {max_retries} attempts: {e}")
                        raise
                    time.sleep(2 ** attempt)  # Exponential backoff
            return None
        return wrapper
    return decorator

@retry_on_failure()
def load_data_from_sqlite() -> Optional[pd.DataFrame]:
    """Load data from SQLite into pandas DataFrame with error handling"""
    try:
        if not DATABASE_PATH:
            raise ValueError("Database path not configured")
            
        conn = sqlite3.connect(DATABASE_PATH, timeout=10)
        
        # Check if table exists
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (TABLE_NAME,))
        if not cursor.fetchone():
            conn.close()
            raise ValueError(f"Table '{TABLE_NAME}' not found in database")
        
        # Load data with proper error handling
        df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", conn)
        conn.close()
        
        if df.empty:
            raise ValueError("Database table is empty")
            
        logger.info(f"Successfully loaded {len(df)} rows from database")
        return df
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        raise Exception(f"Database connection failed: {e}")
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def add_to_chat_history(message_type: str, content: str, sql_query: Optional[str] = None, 
                       chart_type: Optional[str] = None, error: bool = False):
    """Add message to chat history with enhanced metadata"""
    entry = {
        "timestamp": datetime.now().isoformat(),
        "type": message_type,  
        "content": content,
        "sql_query": sql_query,
        "chart_type": chart_type,
        "error": error,
        "session_id": id(st.session_state)
    }
    st.session_state.chat_history.append(entry)

@retry_on_failure()
def get_schema() -> Dict[str, str]:
    """Get database schema with error handling"""
    try:
        conn = sqlite3.connect(DATABASE_PATH, timeout=10)
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({TABLE_NAME});")
        columns = cursor.fetchall()
        conn.close()
        
        if not columns:
            raise ValueError(f"No schema information found for table {TABLE_NAME}")
            
        schema = {col[1]: col[2] for col in columns}
        logger.info(f"Retrieved schema with {len(schema)} columns")
        return schema
        
    except Exception as e:
        logger.error(f"Error getting schema: {e}")
        raise

@retry_on_failure()
def call_groq(messages: List[Dict[str, str]]) -> str:
    """Call GROQ API with robust error handling and validation"""
    try:
        if not GROQ_API_KEY:
            raise ValueError("GROQ API key not configured")
            
        if not messages or not isinstance(messages, list):
            raise ValueError("Invalid messages format")
            
        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json",
        }
        
        payload = {
            "model": GROQ_MODEL,
            "messages": messages,
            "temperature": 0.1,  # Lower temperature for more consistent results
            "max_tokens": 2000
        }
        
        response = requests.post(
            "https://api.groq.com/openai/v1/chat/completions", 
            headers=headers, 
            json=payload,
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code == 429:  # Rate limit
            time.sleep(5)
            raise Exception("Rate limit exceeded, retrying...")
        elif response.status_code != 200:
            raise Exception(f"GROQ API Error {response.status_code}: {response.text}")

        result = response.json()
        
        if 'choices' not in result or not result['choices']:
            raise Exception("Invalid response from GROQ API")
            
        content = result["choices"][0]["message"]["content"].strip()
        
        if not content:
            raise Exception("Empty response from GROQ API")
            
        return content
        
    except requests.exceptions.Timeout:
        raise Exception("API request timed out")
    except requests.exceptions.ConnectionError:
        raise Exception("Failed to connect to GROQ API")
    except Exception as e:
        logger.error(f"GROQ API call failed: {e}")
        raise

def validate_sql_query(sql: str) -> bool:
    """Validate SQL query for safety and correctness"""
    if not sql or not isinstance(sql, str):
        return False
        
    sql_lower = sql.lower().strip()
    
    # Check for dangerous operations
    dangerous_keywords = ['drop', 'delete', 'insert', 'update', 'alter', 'create', 'truncate']
    if any(keyword in sql_lower for keyword in dangerous_keywords):
        return False
    
    # Must be a SELECT statement
    if not sql_lower.startswith('select'):
        return False
        
    # Basic syntax validation
    if sql.count('(') != sql.count(')'):
        return False
        
    return True

def run_sql_query_on_dataframe(sql: str, df: pd.DataFrame) -> Tuple[Optional[List[str]], Any, Optional[pd.DataFrame]]:
    """Execute SQL query with enhanced validation and error handling"""
    try:
        if not sql:
            return None, "Empty SQL query", None
            
        if not validate_sql_query(sql):
            return None, "Invalid or unsafe SQL query", None
            
        if df is None or df.empty:
            return None, "No data available", None
            
        # Connect to database and execute query
        conn = sqlite3.connect(DATABASE_PATH, timeout=10)
        cursor = conn.cursor()
        
        # Set query timeout
        cursor.execute("PRAGMA busy_timeout = 30000")
        
        cursor.execute(sql)
        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        conn.close()
        
        if rows:
            result_df = pd.DataFrame(rows, columns=headers)
            
            # Validate result size
            if len(result_df) > 10000:
                logger.warning(f"Large result set: {len(result_df)} rows")
                
            return headers, rows, result_df
        else:
            return headers, [], pd.DataFrame()
            
    except sqlite3.Error as e:
        error_msg = f"SQL execution error: {str(e)}"
        logger.error(error_msg)
        return None, error_msg, None
    except Exception as e:
        error_msg = f"Query execution failed: {str(e)}"
        logger.error(error_msg)
        return None, error_msg, None

def sql_generation_prompt(user_question: str, schema_dict: Dict[str, str]) -> List[Dict[str, str]]:
    """Generate enhanced SQL generation prompt"""
    schema_str = "\n".join([f"  {col} ({dtype})" for col, dtype in schema_dict.items()])
    
    return [
        {
            "role": "system",
            "content": """You are an expert SQL analyst. Generate only valid SQLite queries that are safe and efficient."""
        },
        {
            "role": "user",
            "content": f"""
Generate a SQLite query for the following question. Follow these rules:

DATABASE SCHEMA:
Table: {TABLE_NAME}
Columns:
{schema_str}

IMPORTANT RULES:
1. ONLY use SELECT statements
2. Use exact column names from the schema
3. Sales columns: NA_Sales, EU_Sales, JP_Sales, Other_Sales, Global_Sales
4. For regional data, use appropriate *_Sales columns (NOT Platform filtering)
5. Include proper ORDER BY for meaningful results
6. Limit results to reasonable numbers (TOP 10-20 for rankings)
7. Handle NULL values appropriately
8. Use LIKE for partial text matching (case-insensitive)
9. For year ranges, use BETWEEN or >= / <=
10. Return only the SQL query, no explanations

User Question: "{user_question}"

SQL Query:"""
        }
    ]

def chart_type_selection_prompt(user_question: str, headers: List[str], sample_data: List[Any]) -> List[Dict[str, str]]:
    """Enhanced chart type selection with better logic"""
    sample_str = "\n".join([str(row)[:100] + "..." if len(str(row)) > 100 else str(row) 
                           for row in sample_data[:3]]) if sample_data else "No data"
    
    return [
        {
            "role": "system",
            "content": "You are a data visualization expert. Select the most appropriate chart type based on data characteristics and user intent."
        },
        {
            "role": "user",
            "content": f"""
User Question: "{user_question}"
Data Columns: {headers}
Sample Data:
{sample_str}

CHART TYPE RULES:
- If user mentions specific chart type (pie, bar, line, etc.), use that
- For time series/years: line or area
- For categorical comparisons: bar
- For proportions/percentages: pie or donut
- For distributions: histogram or box
- For correlations: scatter or heatmap
- For rankings/top items: bar
- For regional comparisons: bar or heatmap

Available types: bar, line, pie, scatter, heatmap, histogram, box, area, donut, violin

Return ONLY the chart type name:"""
        }
    ]

def explanation_prompt(user_question: str, sql_query: str, query_result: List[Any], 
                      headers: List[str], row_count: int) -> List[Dict[str, str]]:
    """Enhanced explanation prompt with better context"""
    rows_preview = "\n".join([str(row) for row in query_result[:5]]) if query_result else "No results"
    
    return [
        {
            "role": "system",
            "content": """You are a senior data analyst providing strategic insights. 
            Be concise, data-driven, and actionable in your analysis."""
        },
        {
            "role": "user",
            "content": f"""
User Question: "{user_question}"
SQL Query Used: {sql_query}
Result Count: {row_count}
Columns: {headers}
Sample Results:
{rows_preview}

Provide strategic insights that:
1. Directly answer the user's question
2. Highlight key findings from the data
3. Mention notable trends or patterns
4. Provide business context where relevant
5. Keep it concise (2-3 paragraphs max)

Analysis:"""
        }
    ]

def get_plot_columns(result_df: pd.DataFrame, chart_type: str) -> Tuple[Optional[str], Optional[str]]:
    """Enhanced column selection for plotting with better type detection"""
    if result_df.empty:
        return None, None
        
    numeric_cols = result_df.select_dtypes(include=[np.number]).columns.tolist()
    text_cols = result_df.select_dtypes(include=['object', 'string']).columns.tolist()
    datetime_cols = result_df.select_dtypes(include=['datetime64']).columns.tolist()
    
    # Try to convert text columns to datetime
    for col in text_cols[:]:
        if 'year' in col.lower() or 'date' in col.lower():
            try:
                pd.to_datetime(result_df[col])
                datetime_cols.append(col)
                text_cols.remove(col)
            except:
                pass
    
    categorical_cols = text_cols + datetime_cols
    
    if chart_type in ['bar', 'line', 'area']:
        x_col = categorical_cols[0] if categorical_cols else result_df.columns[0]
        y_col = numeric_cols[0] if numeric_cols else result_df.columns[-1]
        return x_col, y_col
    
    elif chart_type in ['pie', 'donut']:
        label_col = categorical_cols[0] if categorical_cols else result_df.columns[0]
        value_col = numeric_cols[0] if numeric_cols else result_df.columns[-1]
        return label_col, value_col
    
    elif chart_type == 'scatter':
        if len(numeric_cols) >= 2:
            return numeric_cols[0], numeric_cols[1]
        return None, None
    
    elif chart_type in ['histogram', 'box', 'violin']:
        return None, numeric_cols[0] if numeric_cols else None
    
    return result_df.columns[0], result_df.columns[-1]

def create_advanced_chart(chart_type: str, result_df: pd.DataFrame, user_question: str):
    """Enhanced chart creation with better error handling and styling"""
    if result_df is None or result_df.empty:
        st.warning("⚠️ Cannot create visualization: No data available.")
        return

    try:
        # Set up the plot with better styling
        plt.style.use('default')  # Reset to default to avoid conflicts
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Set better color palette
        colors = plt.cm.Set2(np.linspace(0, 1, 8))
        
        x_col, y_col = get_plot_columns(result_df, chart_type)
        
        if chart_type == "bar":
            if not x_col or not y_col:
                raise ValueError("Missing required columns for bar chart")
            
            # Handle large datasets
            plot_df = result_df.nlargest(15, y_col) if len(result_df) > 15 else result_df
            plot_df = plot_df.sort_values(y_col, ascending=True)  # Ascending for horizontal layout
            
            # Create horizontal bar chart for better readability
            bars = ax.barh(plot_df[x_col].astype(str), plot_df[y_col], color=colors[0])
            ax.set_xlabel(y_col)
            ax.set_ylabel(x_col)
            ax.set_title(f"{y_col} by {x_col}", fontsize=14, fontweight='bold')
            
            # Add value labels
            for i, bar in enumerate(bars):
                width = bar.get_width()
                ax.text(width + 0.01*max(plot_df[y_col]), bar.get_y() + bar.get_height()/2,
                        f'{width:.1f}', ha='left', va='center', fontsize=10)
                
        elif chart_type == "line":
            if not x_col or not y_col:
                raise ValueError("Missing required columns for line chart")
                
            plot_df = result_df.sort_values(x_col).copy()
            
            # Handle datetime conversion
            if plot_df[x_col].dtype == 'object':
                try:
                    plot_df[x_col] = pd.to_datetime(plot_df[x_col])
                except:
                    pass
            
            ax.plot(plot_df[x_col], plot_df[y_col], 
                    marker='o', linewidth=3, markersize=8, color=colors[1])
            ax.set_xlabel(x_col)
            ax.set_ylabel(y_col)
            ax.set_title(f"{y_col} Trend by {x_col}", fontsize=14, fontweight='bold')
            ax.grid(True, alpha=0.3)
            
            # Rotate x-axis labels if needed
            if plot_df[x_col].dtype == 'object':
                plt.xticks(rotation=45, ha='right')
                
        elif chart_type in ["pie", "donut"]:
            if not x_col or not y_col:
                raise ValueError("Missing required columns for pie chart")
                
            # Handle large datasets - show top categories and group others
            if len(result_df) > 8:
                plot_df = result_df.nlargest(7, y_col).copy()
                others_sum = result_df.iloc[7:][y_col].sum()
                if others_sum > 0:
                    others_row = pd.DataFrame({x_col: ['Others'], y_col: [others_sum]})
                    plot_df = pd.concat([plot_df, others_row], ignore_index=True)
            else:
                plot_df = result_df.copy()
            
            labels = plot_df[x_col].astype(str)
            sizes = plot_df[y_col]
            
            # Filter out zero or negative values
            mask = sizes > 0
            labels = labels[mask]
            sizes = sizes[mask]
            
            if len(sizes) == 0:
                raise ValueError("No positive values for pie chart")
            
            wedge_props = dict(width=0.7) if chart_type == "donut" else {}
            
            wedges, texts, autotexts = ax.pie(sizes, labels=labels, autopct='%1.1f%%',
                                            colors=colors, startangle=90, 
                                            wedgeprops=wedge_props)
            ax.set_title(f"Distribution of {y_col} by {x_col}", fontsize=14, fontweight='bold')
            
        elif chart_type == "scatter":
            if not x_col or not y_col:
                raise ValueError("Missing required numeric columns for scatter plot")
            
            # Add some jitter if values are discrete
            x_vals = result_df[x_col]
            y_vals = result_df[y_col]
            
            ax.scatter(x_vals, y_vals, alpha=0.7, s=80, c=colors[2], edgecolors='black', linewidth=0.5)
            ax.set_xlabel(x_col)
            ax.set_ylabel(y_col)
            ax.set_title(f"{y_col} vs {x_col}", fontsize=14, fontweight='bold')
            ax.grid(True, alpha=0.3)
            
        elif chart_type == "heatmap":
            numeric_cols = result_df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) < 2:
                raise ValueError("Need at least 2 numeric columns for heatmap")
            
            corr_matrix = result_df[numeric_cols].corr()
            
            # Use better colormap and styling
            mask = np.triu(np.ones_like(corr_matrix, dtype=bool))  # Show only lower triangle
            sns.heatmap(corr_matrix, mask=mask, annot=True, fmt='.2f', 
                       cmap='RdYlBu_r', center=0, square=True, ax=ax,
                       cbar_kws={"shrink": .8})
            ax.set_title("Correlation Heatmap", fontsize=14, fontweight='bold')
            
        elif chart_type == "histogram":
            if not y_col:
                raise ValueError("Missing numeric column for histogram")
            
            values = result_df[y_col].dropna()
            if len(values) == 0:
                raise ValueError("No valid values for histogram")
            
            n_bins = min(30, max(10, int(np.sqrt(len(values)))))
            ax.hist(values, bins=n_bins, alpha=0.7, color=colors[3], 
                   edgecolor='black', linewidth=0.5)
            ax.set_xlabel(y_col)
            ax.set_ylabel("Frequency")
            ax.set_title(f"Distribution of {y_col}", fontsize=14, fontweight='bold')
            ax.grid(True, alpha=0.3)
            
        elif chart_type in ["box", "violin"]:
            if not y_col:
                raise ValueError("Missing numeric column for box/violin plot")
            
            values = result_df[y_col].dropna()
            if len(values) == 0:
                raise ValueError("No valid values for box/violin plot")
            
            if chart_type == "box":
                ax.boxplot([values], labels=[y_col], patch_artist=True,
                          boxprops=dict(facecolor=colors[4]))
            else:  # violin
                parts = ax.violinplot([values], positions=[1], showmeans=True, showmedians=True)
                for pc in parts['bodies']:
                    pc.set_facecolor(colors[4])
                ax.set_xticks([1])
                ax.set_xticklabels([y_col])
            
            ax.set_title(f"{chart_type.title()} Plot of {y_col}", fontsize=14, fontweight='bold')
            ax.grid(True, alpha=0.3)
            
        elif chart_type == "area":
            if not x_col or not y_col:
                raise ValueError("Missing required columns for area chart")
            
            plot_df = result_df.sort_values(x_col).copy()
            ax.fill_between(range(len(plot_df)), plot_df[y_col], 
                           alpha=0.6, color=colors[5])
            ax.plot(range(len(plot_df)), plot_df[y_col], 
                   color='darkred', linewidth=2)
            ax.set_xlabel("Data Points")
            ax.set_ylabel(y_col)
            ax.set_title(f"{y_col} Area Chart", fontsize=14, fontweight='bold')
            ax.grid(True, alpha=0.3)
            
        else:
            # Fallback to bar chart
            return create_advanced_chart("bar", result_df, user_question)
        
        plt.tight_layout()
        st.pyplot(fig)
        plt.close()
        
    except Exception as e:
        error_msg = f"Failed to create {chart_type} chart: {str(e)}"
        logger.error(error_msg)
        st.error(f"❌ {error_msg}")
        
        # Try fallback to simple bar chart
        if chart_type != "bar" and not result_df.empty:
            st.info("🔄 Attempting fallback visualization...")
            try:
                create_simple_fallback_chart(result_df)
            except Exception as fallback_error:
                st.error(f"❌ Fallback chart also failed: {str(fallback_error)}")

def create_simple_fallback_chart(result_df: pd.DataFrame):
    """Simple fallback chart when main chart creation fails"""
    try:
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Get first numeric column for y-axis
        numeric_cols = result_df.select_dtypes(include=[np.number]).columns
        text_cols = result_df.select_dtypes(include=['object']).columns
        
        if len(numeric_cols) > 0 and len(text_cols) > 0:
            x_data = result_df[text_cols[0]].astype(str).iloc[:10]  # Limit to 10 items
            y_data = result_df[numeric_cols[0]].iloc[:10]
            
            ax.bar(x_data, y_data, color='lightblue', alpha=0.7)
            ax.set_xlabel(text_cols[0])
            ax.set_ylabel(numeric_cols[0])
            ax.set_title("Data Overview")
            plt.xticks(rotation=45, ha='right')
            
        elif len(numeric_cols) > 0:
            # Just plot the numeric data
            ax.plot(result_df[numeric_cols[0]].iloc[:20], marker='o')
            ax.set_ylabel(numeric_cols[0])
            ax.set_title("Data Trend")
            
        plt.tight_layout()
        st.pyplot(fig)
        plt.close()
        
    except Exception as e:
        st.error(f"Even fallback chart failed: {e}")

def sanitize_sql(raw_sql: str) -> str:
    """Enhanced SQL sanitization"""
    if not raw_sql:
        return ""
        
    # Remove code block markers
    cleaned = re.sub(r'```(?:sql)?\s*', '', raw_sql)
    cleaned = re.sub(r'```\s*$', '', cleaned)
    
    # Remove extra whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    # Ensure it ends with semicolon if it doesn't
    if cleaned and not cleaned.endswith(';'):
        cleaned += ';'
    
    return cleaned

def display_chat_message(message_type: str, content: str, sql_query: Optional[str] = None, 
                        chart_type: Optional[str] = None, error: bool = False):
    """Enhanced chat message display with better formatting"""
    if message_type == "user":
        with st.chat_message("user"):
            st.write(content)
            
    elif message_type == "assistant":
        with st.chat_message("assistant"):
            if error:
                st.error(content)
            else:
                st.write(content)
                
            if sql_query and st.session_state.show_debug:
                with st.expander("🐛 Debug - SQL Query"):
                    st.code(sql_query, language='sql')
                    
            if chart_type:
                st.info(f"📊 Generated {chart_type.title()} Chart")
                
    elif message_type == "system":
        with st.chat_message("assistant"):
            st.info(content)

def validate_database_connection() -> bool:
    """Validate database connection and structure"""
    try:
        conn = sqlite3.connect(DATABASE_PATH, timeout=5)
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (TABLE_NAME,))
        if not cursor.fetchone():
            conn.close()
            return False
        
        # Check if table has data
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        count = cursor.fetchone()[0]
        
        conn.close()
        
        return count > 0
        
    except Exception as e:
        logger.error(f"Database validation failed: {e}")
        return False

def main():
    """Main application with enhanced error handling"""
    try:
        st.set_page_config(
            page_title="🎮 Game Sales Chat Assistant",
            page_icon="🎮",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        initialize_session_state()
        
        st.title("🎮 Game Sales Chat Assistant")
        st.markdown("""
        **Ask me anything about game sales data!** I can generate SQL queries, create visualizations, and provide strategic insights.
        
        *Examples: "Show top 10 best selling games", "Which platform has highest sales in Japan?", "Create a pie chart of sales by genre"*
        """)
        
        # Check database connection first
        if not st.session_state.data_loaded:
            with st.spinner("🔍 Checking database connection..."):
                if not validate_database_connection():
                    st.error("""
                    ❌ **Database Connection Failed**
                    
                    Please ensure:
                    - Database file 'games.db' exists
                    - Table 'game_sales' contains data
                    - File permissions are correct
                    """)
                    st.stop()
        
        # Load data
        if st.session_state.df is None:
            with st.spinner("📊 Loading game sales data..."):
                try:
                    st.session_state.df = load_data_from_sqlite()
                    st.session_state.schema = get_schema()
                    st.session_state.data_loaded = True
                    st.success(f"✅ Loaded {len(st.session_state.df):,} game records")
                except Exception as e:
                    st.error(f"❌ Failed to load data: {e}")
                    st.info("Please check your database configuration and try again.")
                    st.stop()
        
        # Sidebar
        with st.sidebar:
            st.header("🛠️ Controls")
            
            # Debug toggle
            debug_enabled = st.toggle("🐛 Debug Mode", value=st.session_state.show_debug,
                                    help="Show SQL queries and technical details")
            if debug_enabled != st.session_state.show_debug:
                st.session_state.show_debug = debug_enabled
                st.rerun()
            
            st.markdown("---")
            
            # Database info
            st.header("📊 Database Info")
            if st.session_state.df is not None:
                st.metric("Total Records", f"{len(st.session_state.df):,}")
                st.metric("Columns", len(st.session_state.schema))
                
                if st.session_state.schema:
                    with st.expander("Schema Details"):
                        for col, dtype in st.session_state.schema.items():
                            st.text(f"• {col} ({dtype})")
            
            st.markdown("---")
            
            # Action buttons
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("🗑️ Clear Chat", help="Clear chat history"):
                    st.session_state.chat_history = []
                    st.rerun()
            
            with col2:
                if st.button("📊 Data Preview", help="Show data sample"):
                    st.session_state.show_preview = not st.session_state.get('show_preview', False)
                    st.rerun()
            
            # Error statistics
            if st.session_state.error_count > 0:
                st.markdown("---")
                st.header("⚠️ Session Stats")
                st.metric("Errors", st.session_state.error_count)
            
            # Sample queries
            st.markdown("---")
            st.header("💡 Sample Queries")
            
            sample_queries = [
                "Top 10 best selling games worldwide",
                "Sales by platform in North America",
                "Show me a pie chart of genre distribution",
                "Which year had the highest game sales?",
                "Compare Nintendo vs Sony sales",
                "Show sports games sales trend over time"
            ]
            
            for query in sample_queries:
                if st.button(f"📝 {query}", key=f"sample_{hash(query)}", use_container_width=True):
                    # Add to chat and process
                    st.session_state.sample_query = query
                    st.rerun()
        
        # Main content area
        
        # Show data preview if requested
        if st.session_state.get('show_preview', False) and st.session_state.df is not None:
            with st.expander("📊 Data Preview", expanded=True):
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.dataframe(st.session_state.df.head(10), use_container_width=True)
                with col2:
                    st.metric("Rows", len(st.session_state.df))
                    st.metric("Columns", len(st.session_state.df.columns))
                    
                    # Data quality info
                    missing_data = st.session_state.df.isnull().sum().sum()
                    if missing_data > 0:
                        st.metric("Missing Values", missing_data)
        
        # Chat history display
        for message in st.session_state.chat_history:
            display_chat_message(
                message["type"], 
                message["content"], 
                message.get("sql_query"), 
                message.get("chart_type"),
                message.get("error", False)
            )
        
        # Handle sample query
        if st.session_state.get('sample_query'):
            prompt = st.session_state.sample_query
            del st.session_state.sample_query
            process_user_query(prompt)
        
        # Chat input
        if prompt := st.chat_input("Ask me about game sales data... 🎮"):
            process_user_query(prompt)
    
    except Exception as e:
        st.error(f"❌ Application Error: {e}")
        logger.error(f"Main application error: {e}")
        
        if st.button("🔄 Restart Application"):
            st.rerun()

def process_user_query(prompt: str):
    """Process user query with comprehensive error handling"""
    try:
        # Add user message to chat
        add_to_chat_history("user", prompt)
        display_chat_message("user", prompt)
        
        # Validate input
        if not prompt or len(prompt.strip()) < 3:
            error_msg = "⚠️ Please provide a more specific question about the game sales data."
            st.warning(error_msg)
            add_to_chat_history("assistant", error_msg, error=True)
            return
        
        if len(prompt) > 500:
            error_msg = "⚠️ Question is too long. Please keep it under 500 characters."
            st.warning(error_msg)
            add_to_chat_history("assistant", error_msg, error=True)
            return
        
        with st.chat_message("assistant"):
            with st.spinner("🤔 Analyzing your question..."):
                try:
                    # Step 1: Generate SQL query
                    sql_prompt = sql_generation_prompt(prompt, st.session_state.schema)
                    raw_sql = call_groq(sql_prompt)
                    sql_query = sanitize_sql(raw_sql)
                    
                    if not sql_query:
                        raise ValueError("Failed to generate valid SQL query")
                    
                    if st.session_state.show_debug:
                        st.code(sql_query, language='sql')
                    
                    # Step 2: Execute query
                    with st.spinner("📊 Executing query..."):
                        headers, result, result_df = run_sql_query_on_dataframe(sql_query, st.session_state.df)
                        
                        if headers is None:
                            error_msg = f"❌ SQL Execution Failed: {result}"
                            st.error(error_msg)
                            add_to_chat_history("assistant", error_msg, sql_query, error=True)
                            st.session_state.error_count += 1
                            return
                        
                        if not result:
                            no_data_msg = """
                            ⚠️ **No data found for your query.**
                            
                            Try rephrasing your question or asking about:
                            - Different time periods
                            - Specific platforms, genres, or publishers
                            - Alternative metrics (sales, rankings, etc.)
                            """
                            st.warning(no_data_msg)
                            add_to_chat_history("assistant", no_data_msg, sql_query)
                            return
                    
                    # Step 3: Display results
                    st.success(f"✅ Found {len(result):,} results!")
                    
                    # Show data table with pagination for large results
                    if len(result_df) > 20:
                        st.info(f"Showing first 20 of {len(result_df)} results")
                        st.dataframe(result_df.head(20), use_container_width=True)
                        
                        with st.expander("📋 View All Results"):
                            st.dataframe(result_df, use_container_width=True)
                    else:
                        st.dataframe(result_df, use_container_width=True)
                    
                    # Step 4: Generate visualization if requested
                    chart_type = None
                    wants_chart = any(word in prompt.lower() for word in 
                                    ['chart', 'graph', 'plot', 'visualiz', 'heatmap', 'pie', 
                                     'bar', 'line', 'show', 'display', 'draw'])
                    
                    if wants_chart and len(result) > 0:
                        with st.spinner("📊 Creating visualization..."):
                            try:
                                chart_prompt = chart_type_selection_prompt(prompt, headers, result)
                                chart_type = call_groq(chart_prompt).strip().lower()
                                
                                # Validate chart type
                                valid_charts = ['bar', 'line', 'pie', 'scatter', 'heatmap', 
                                              'histogram', 'box', 'area', 'donut', 'violin']
                                if chart_type not in valid_charts:
                                    chart_type = 'bar'  # Default fallback
                                
                                st.subheader(f"📊 {chart_type.title()} Chart")
                                create_advanced_chart(chart_type, result_df, prompt)
                                
                            except Exception as chart_error:
                                st.warning(f"⚠️ Could not create visualization: {chart_error}")
                                logger.error(f"Chart creation failed: {chart_error}")
                    
                    # Step 5: Generate insights
                    with st.spinner("🧠 Generating strategic insights..."):
                        try:
                            explanation = call_groq(
                                explanation_prompt(prompt, sql_query, result, headers, len(result))
                            ).strip()
                            
                            if explanation:
                                st.subheader("🧠 Strategic Insights")
                                st.markdown(explanation)
                            else:
                                st.info("Analysis completed - see results above.")
                                
                        except Exception as explanation_error:
                            st.warning("⚠️ Could not generate insights, but your data is ready above.")
                            logger.error(f"Explanation generation failed: {explanation_error}")
                    
                    # Success message for chat history
                    success_parts = [f"Found {len(result):,} results"]
                    if chart_type:
                        success_parts.append(f"generated {chart_type} chart")
                    success_parts.append("provided analysis")
                    
                    response_msg = "✅ " + ", ".join(success_parts) + "."
                    add_to_chat_history("assistant", response_msg, sql_query, chart_type)
                    
                except Exception as e:
                    error_msg = f"❌ Processing failed: {str(e)}"
                    st.error(error_msg)
                    add_to_chat_history("assistant", error_msg, error=True)
                    st.session_state.error_count += 1
                    logger.error(f"Query processing failed: {e}")
                    
                    # Provide helpful suggestions
                    st.info("""
                    **Try these alternatives:**
                    - Simplify your question
                    - Ask about specific games, platforms, or genres
                    - Use different keywords (e.g., "sales", "top", "best")
                    - Check the sample queries in the sidebar
                    """)
    
    except Exception as e:
        st.error(f"❌ Unexpected error: {e}")
        logger.error(f"Unexpected error in process_user_query: {e}")
        st.session_state.error_count += 1

if __name__ == "__main__":
    main()