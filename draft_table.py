import streamlit as st
import datetime
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import base64
import datetime
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import datetime
import base64

st.set_page_config(layout="wide")

HOST = "localhost"
PORT = 8501
PROTO = "http"

# Parse URL parameters
params = st.query_params


def get_minimal_timestamp_format(target_timestamp):
    """
    Get the current timestamp in nanoseconds, microseconds, milliseconds, and seconds formats,
    then find the minimal distance to the target timestamp and return the appropriate format.

    :param target_timestamp: The target timestamp to compare against (in seconds).
    :return: The closest timestamp format ('ns', 'us', 'ms', 's').
    """
    now_ns = datetime.datetime.now().timestamp() * 1e9
    now_us = datetime.datetime.now().timestamp() * 1e6
    now_ms = datetime.datetime.now().timestamp() * 1e3
    now_s = datetime.datetime.now().timestamp()

    distances = {
        "ns": abs(now_ns - target_timestamp),
        "us": abs(now_us - target_timestamp),
        "ms": abs(now_ms - target_timestamp),
        "s": abs(now_s - target_timestamp),
    }

    closest_format = min(distances, key=distances.get)
    return closest_format


if "df" not in st.session_state:
    st.session_state.df = pd.DataFrame(
        np.random.randn(12, 5), columns=["a", "b", "c", "d", "e"]
    )

def color_cells(val):
    """
    Color cells based on their value.
    Red for 'Sell', Green for 'Buy'.
    """
    if isinstance(val, str):
        if 'Sell' in val:
            return 'color: red'
        elif 'Buy' in val:
            return 'color: green'
    return ''

styled_df = st.session_state.df.style.applymap(color_cells)
st.dataframe(styled_df, use_container_width=True)


# Decode base64 encoded connection and query if provided
if "data" in params:
    data_str = base64.b64decode(params["data"]).decode("utf-8")
    data = eval(data_str)
else:
    data = {}


if 'query' not in st.session_state: st.session_state.query = data.get("query", "SELECT * FROM your_table LIMIT 10;")
query = st.text_area("SQL Query", key="query")

if 'db_host' not in st.session_state: st.session_state.db_host = data.get("db_host", "localhost")
if 'db_port' not in st.session_state: st.session_state.db_port = int(data.get("db_port", 5432))
if 'db_user' not in st.session_state: st.session_state.db_user = data.get("db_user", "your_username")
if 'db_password' not in st.session_state: st.session_state.db_password = data.get("db_password", "your_password")
if 'db_name' not in st.session_state: st.session_state.db_name = data.get("db_name", "your_database")


def update_df():
    data = {
        "db_host": st.session_state.db_host,
        "db_port": int(st.session_state.db_port),
        "db_user": st.session_state.db_user,
        "db_password": st.session_state.db_password,
        "db_name": st.session_state.db_name,
        "query": query,
    }
    encoded_data_str = base64.b64encode(str(data).encode("utf-8")).decode("utf-8")

    st.write("query link:")
    pl=f"{PROTO}://{HOST}:{PORT}/?data={encoded_data_str}"
    st.page_link(pl, label=pl)

    connection_string = f"postgresql://{data['db_user']}:{data['db_password']}@{data['db_host']}:{data['db_port']}/{data['db_name']}"
    engine = create_engine(connection_string)
    df = pd.read_sql(query, engine)

    # Convert timestamp columns from nanoseconds to datetime
    for col in df.columns:
        if "timestamp" in col.lower():
            df[col] = pd.to_datetime(
                df[col],
                unit=get_minimal_timestamp_format(df[col][0]),
                # format="%d/%m",
            )

    st.session_state.df = df


# Connect to the PostgreSQL database and execute the query
if st.button("Run Query"):
    try:
        update_df()
        # db_host = st.session_state.get("db_host", "localhost")
        # db_port = st.session_state.get("db_port", 5432)
        # db_user = st.session_state.get("db_user", "your_username")
        # db_password = st.session_state.get("db_password", "your_password")
        # db_name = st.session_state.get("db_name", "your_database")
        # query = st.session_state.get("query", "SELECT * FROM your_table LIMIT 10;")
        # Encode current inputs into a single base64 string

        # Store input values in session state
        # st.session_state.db_host = db_host
        # st.session_state.db_port = db_port
        # st.session_state.db_user = db_user
        # st.session_state.db_password = db_password
        # st.session_state.db_name = db_name
        # st.session_state.query = query
    except Exception as e:
        st.error(f"Error: {e}")


st.text_input("Database Host", key="db_host")
st.number_input("Database Port", key="db_port", format='%u')
st.text_input("Database User", key="db_user")
st.text_input("Database Password", type="password", key="db_password")
st.text_input("Database Name", key="db_name")
