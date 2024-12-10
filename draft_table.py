import streamlit as st
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import pandas as pd
import numpy as np

st.set_page_config(layout="wide")

# Database connection inputs
db_host = st.text_input("Database Host", value=st.session_state.get('db_host', "localhost"))
db_port = st.number_input("Database Port", value=st.session_state.get('db_port', 5432))
db_user = st.text_input("Database User", value=st.session_state.get('db_user', "your_username"))
db_password = st.text_input("Database Password", type="password", value=st.session_state.get('db_password', "your_password"))
db_name = st.text_input("Database Name", value=st.session_state.get('db_name', "your_database"))

# Query input
query = st.text_area("SQL Query", value=st.session_state.get('query', "SELECT * FROM your_table LIMIT 10;"))

# Connect to the PostgreSQL database and execute the query
if st.button("Run Query"):
    try:
        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)
        df = pd.read_sql(query, engine)
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
