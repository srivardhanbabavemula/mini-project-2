# streamlit_app2.py (top of file)
import sys

try:
    from google import genai
except Exception as e:
    # Clear, actionable error for deploy logs / console
    raise ImportError(
        "Missing or broken Google GenAI SDK. Ensure `google-genai` is installed "
        "in the runtime and no local module named `google` exists in your repo. "
        f"Original error: {e}"
    )
import re
import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from google import genai  # Google GenAI SDK for Gemini
from google.genai.errors import APIError
import os
import bcrypt

load_dotenv()

# --- Configuration for Gemini API ---
GEMINI_API_KEY  = st.secrets["OPENAI_API_KEY"]
HASHED_PASSWORD = st.secrets["HASHED_PASSWORD"].encode("utf-8")
# --- End Configuration ---

DATABASE_SCHEMA = """
Database Schema:

LOOKUP TABLES:
- region (regionid SERIAL PRIMARY KEY, region TEXT NOT NULL)
- country (countryid SERIAL PRIMARY KEY, country TEXT NOT NULL, regionid INTEGER FK to region)
- productcategory (productcategoryid SERIAL PRIMARY KEY, productcategory TEXT NOT NULL, productcategorydescription TEXT NOT NULL)

CORE TABLES:
- customer (
    customerid SERIAL PRIMARY KEY,
    firstname TEXT NOT NULL,
    lastname TEXT NOT NULL,
    address TEXT NOT NULL,
    city TEXT NOT NULL,
    countryid INTEGER FK to country
)

- product (
    productid SERIAL PRIMARY KEY,
    productname TEXT NOT NULL,
    productunitprice REAL NOT NULL,
    productcategoryid INTEGER FK to productcategory
)

- orderdetail (
    orderid SERIAL PRIMARY KEY,
    customerid INTEGER FK to customer,
    productid INTEGER FK to product,
    orderdate DATE NOT NULL,
    quantityordered INTEGER NOT NULL
)

Important Notes:
- Use JOINs to get descriptive names from foreign keys
- orderdate is DATE type
- Use aggregations (SUM, COUNT, AVG) to answer sales questions
- Add LIMIT clauses where needed
"""

# ------------------------
# Login Functions
# ------------------------
def login_screen():
    st.title("🔐 Secure Login")
    st.markdown("---")
    st.write("Enter your password to access the AI SQL Query Assistant.")
    password = st.text_input("Password", type="password", key="login_password")
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        login_btn = st.button("🔓 Login", type="primary", use_container_width=True)
    if login_btn:
        if password:
            try:
                if bcrypt.checkpw(password.encode('utf-8'), HASHED_PASSWORD):
                    st.session_state.logged_in = True
                    st.success("✅ Authentication successful! Redirecting...")
                    st.rerun()
                else:
                    st.error("❌ Incorrect password")
            except Exception as e:
                st.error(f"❌ Authentication error: {e}")
        else:
            st.warning("⚠️ Please enter a password")
    st.markdown("---")
    st.info("""
    **Security Notice:**
    - Passwords are protected using bcrypt hashing
    - Your session is secure and isolated
    - You will remain logged in until you close the browser or click logout
    """)
 
def require_login():
    if "logged_in" not in st.session_state or not st.session_state.logged_in:
        login_screen()
        st.stop()

# ------------------------    
# Database Functions
# ------------------------
@st.cache_resource
def get_db_url():
    return f"postgresql://{st.secrets['POSTGRES_USERNAME']}:{st.secrets['POSTGRES_PASSWORD']}@{st.secrets['POSTGRES_SERVER']}/{st.secrets['POSTGRES_DATABASE']}"

DATABASE_URL = get_db_url()

@st.cache_resource
def get_db_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return None

def run_query(sql):
    conn = get_db_connection()
    if conn is None: return None
    try:
        return pd.read_sql_query(sql, conn)
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return None

# ------------------------
# Gemini Functions
# ------------------------
@st.cache_resource
def get_gemini_client():
    return genai.Client(api_key=GEMINI_API_KEY )

def extract_sql_from_response(response_text):
    return re.sub(r"^```sql\s*|\s*```$", "", response_text, flags=re.IGNORECASE | re.MULTILINE).strip()

def generate_sql_with_gemini(user_question):
    client = get_gemini_client()
    prompt = f"""
You are a PostgreSQL expert. Given the following database schema and a user's question, generate a valid PostgreSQL query.

{DATABASE_SCHEMA}

User Question: {user_question}

Requirements:
1. Generate ONLY the SQL query that can be directly executed.
2. Use proper JOINs for descriptive names.
3. Use COUNT, SUM, AVG as needed.
4. Add LIMIT clauses for queries returning many rows (default 100).
5. Proper date/time functions for DATE columns.
6. Add column aliases using AS.
"""
    try:
        response = client.models.generate_content(
            model="gemini-2.5-pro",
            contents=prompt,
            config=genai.types.GenerateContentConfig(
                temperature=0.1,
                system_instruction="Output ONLY raw SQL query."
            )
        )
        return extract_sql_from_response(response.text)
    except APIError as e:
        st.error(f"Gemini API error: {e}")
        return None
    except Exception as e:
        st.error(f"Unexpected error: {e}")
        return None

# ------------------------
# Main App
# ------------------------
def main():
    require_login()
    
    st.title("🤖 AI SQL Query Assistant (Powered by Gemini)")
    st.markdown("Ask questions in plain English, and the AI will generate SQL for you to review and run!")

    # Sidebar info & logout
    st.sidebar.title("💡 Example Questions")
    st.sidebar.info("""
- Total sales by product category
- Customers by region
- Average order quantity per product
- Top 10 products by sales quantity
- Orders from a specific country
""")
    st.sidebar.markdown("---")
    if st.sidebar.button("🚪 Logout"):
        st.session_state.logged_in = False
        st.rerun()

    # Session state init
    if 'query_history' not in st.session_state:
        st.session_state.query_history = []
    if 'generated_sql' not in st.session_state:
        st.session_state.generated_sql = None
    if 'current_question' not in st.session_state:
        st.session_state.current_question = None

    # User input
    user_question = st.text_area("Ask a question about the database:", height=100, placeholder="e.g. Top 5 products by sales?")
    col1, col2 = st.columns([1,1])
    generate_button = col1.button("Generate SQL", use_container_width=True)
    clear_button = col2.button("Clear History", use_container_width=True)
    
    if clear_button:
        st.session_state.query_history = []
        st.session_state.generated_sql = None
        st.session_state.current_question = None

    if generate_button and user_question:
        user_question = user_question.strip()
        if st.session_state.current_question != user_question:
            st.session_state.generated_sql = None
            st.session_state.current_question = None
        with st.spinner("🧠 Generating SQL with Gemini..."):
            sql_query = generate_sql_with_gemini(user_question)
            if sql_query:
                st.session_state.generated_sql = sql_query
                st.session_state.current_question = user_question

    # Show generated SQL
    if st.session_state.generated_sql:
        with st.expander("Generated SQL Query", expanded=True):
            st.info(f"Question: {st.session_state.current_question}")
            edited_sql = st.text_area("Review/Edit SQL:", value=st.session_state.generated_sql, height=200)
            if st.button("Run Query"):
                with st.spinner("Executing query ..."):
                    df = run_query(edited_sql)
                    if df is not None:
                        st.session_state.query_history.append({
                            'question': user_question,
                            'sql': edited_sql,
                            'rows': len(df)
                        })
                        st.success(f"✅ Query returned {len(df)} rows")
                        st.dataframe(df, use_container_width=True)

    # Query History
    if st.session_state.query_history:
        st.markdown("---")
        st.subheader("📜 Query History (Last 5)")
        for idx, item in enumerate(reversed(st.session_state.query_history[-5:])):
            with st.expander(f"{item['question'][:60]}..."):
                st.code(item['sql'], language="sql")
                st.caption(f"Returned {item['rows']} rows")
                if st.button(f"Re-run", key=f"rerun_{idx}"):
                    df = run_query(item['sql'])
                    if df is not None:
                        st.dataframe(df, use_container_width=True)

if __name__ == "__main__":
    main()
