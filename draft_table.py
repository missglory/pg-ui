import streamlit as st
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import datetime
import base64

st.set_page_config(layout="wide")

# Parse URL parameters
params = st.experimental_get_query_params()

# Decode base64 encoded connection string and query if provided
if 'conn' in params:
    conn_str = base64.b64decode(params['conn'][0]).decode('utf-8')
    db_host, db_port, db_user, db_password, db_name = conn_str.split(',')
else:
    db_host = st.session_state.get('db_host', "localhost")
    db_port = st.session_state.get('db_port', 5432)
    db_user = st.session_state.get('db_user', "your_username")
    db_password = st.session_state.get('db_password', "your_password")
    db_name = st.session_state.get('db_name', "your_database")

if 'query' in params:
    query = base64.b64decode(params['query'][0]).decode('utf-8')
else:
    query = st.session_state.get('query', "SELECT * FROM your_table LIMIT 10;")

# Database connection inputs with URL params as defaults
db_host = st.text_input("Database Host", value=db_host)
db_port = st.number_input("Database Port", value=int(db_port))
db_user = st.text_input("Database User", value=db_user)
db_password = st.text_input("Database Password", type="password", value=db_password)
db_name = st.text_input("Database Name", value=db_name)

# Query input
query = st.text_area("SQL Query", value=query)

# Connect to the PostgreSQL database and execute the query
if st.button("Run Query"):
    try:
        # Encode current inputs into base64 string
        conn_str = ','.join([db_host, str(db_port), db_user, db_password, db_name])
        encoded_conn_str = base64.b64encode(conn_str.encode('utf-8')).decode('utf-8')
        encoded_query = base64.b64encode(query.encode('utf-8')).decode('utf-8')

        st.write(f"Connection String (Base64): {encoded_conn_str}")
        st.write(f"Query (Base64): {encoded_query}")

        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)
        df = pd.read_sql(query, engine)

        # Convert timestamp columns from nanoseconds to datetime
        for col in df.columns:
            if 'timestamp' in col.lower():
                df[col] = pd.to_datetime(df[col], unit='ms')

        st.session_state.df = df

        # Store input values in session state
        st.session_state.db_host = db_host
        st.session_state.db_port = db_port
        st.session_state.db_user = db_user
        st.session_state.db_password = db_password
        st.session_state.db_name = db_name
        st.session_state.query = query

    except Exception as e:
        st.error(f"Error: {e}")

if "df" not in st.session_state:
    st.session_state.df = pd.DataFrame(
        np.random.randn(12, 5), columns=["a", "b", "c", "d", "e"]
    )

st.dataframe(st.session_state.df, use_container_width=True)
