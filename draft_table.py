import streamlit as st
import datetime
import psycopg2
import sqlite3
from sqlalchemy import create_engine, inspect
import pandas as pd
import numpy as np
import base64
import time
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

st.write(params)
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

# styled_df = st.session_state.df.style.applymap(color_cells)
st.dataframe(st.session_state.df, use_container_width=True)


# Decode base64 encoded connection and query if provided
if "data" in params:
    data_str = base64.b64decode(params["data"]).decode("utf-8")
    data = eval(data_str)
else:
    data = {}

if 'query' not in st.session_state: st.session_state.query = data.get("query", "SELECT * FROM your_table LIMIT 10;")
query = st.text_area("SQL Query", key="query")

if 'db_mode' not in st.session_state: st.session_state.db_mode = data.get("db_mode", "postgres")
db_mode = st.selectbox("Database Mode", options=["postgres", "sqlite", "mysql"], index=0, key="db_mode")

if db_mode == "postgres":
    if 'db_host' not in st.session_state: st.session_state.db_host = data.get("db_host", "localhost")
    if 'db_port' not in st.session_state: st.session_state.db_port = int(data.get("db_port", 5432))
    if 'db_user' not in st.session_state: st.session_state.db_user = data.get("db_user", "your_username")
    if 'db_password' not in st.session_state: st.session_state.db_password = data.get("db_password", "your_password")
    if 'db_name' not in st.session_state: st.session_state.db_name = data.get("db_name", "your_database")

elif db_mode == "sqlite":
    if 'db_path' not in st.session_state: st.session_state.db_path = data.get("db_path", "your_database.db")

elif db_mode == "mysql":
    if 'db_host' not in st.session_state: st.session_state.db_host = data.get("db_host", "localhost")
    if 'db_port' not in st.session_state: st.session_state.db_port = int(data.get("db_port", 3306))
    if 'db_user' not in st.session_state: st.session_state.db_user = data.get("db_user", "your_username")
    if 'db_password' not in st.session_state: st.session_state.db_password = data.get("db_password", "your_password")
    if 'db_name' not in st.session_state: st.session_state.db_name = data.get("db_name", "your_database")


def update_df():
    data = {
        "db_mode": db_mode,
    }
    if db_mode == "postgres":
        data["db_host"] = st.session_state.db_host
        data["db_port"] = int(st.session_state.db_port)
        data["db_user"] = st.session_state.db_user
        data["db_password"] = st.session_state.db_password
        data["db_name"] = st.session_state.db_name
    elif db_mode == "sqlite":
        data["db_path"] = st.session_state.db_path
    elif db_mode == "mysql":
        data["db_host"] = st.session_state.db_host
        data["db_port"] = int(st.session_state.db_port)
        data["db_user"] = st.session_state.db_user
        data["db_password"] = st.session_state.db_password
        data["db_name"] = st.session_state.db_name
    data["query"] = query
    encoded_data_str = base64.b64encode(str(data).encode("utf-8")).decode("utf-8")

    st.write("query link:")
    pl=f"{PROTO}://{HOST}:{PORT}/?data={encoded_data_str}"
    st.page_link(pl, label=pl)

    if db_mode == "postgres":
        connection_string = f"postgresql://{st.session_state.db_user}:{st.session_state.db_password}@{st.session_state.db_host}:{st.session_state.db_port}/{st.session_state.db_name}"
        engine = create_engine(connection_string)
    elif db_mode == "sqlite":
        engine = create_engine(f"sqlite:///{st.session_state.db_path}")
    elif db_mode == "mysql":
        connection_string = f"mysql+pymysql://{st.session_state.db_user}:{st.session_state.db_password}@{st.session_state.db_host}:{st.session_state.db_port}/{st.session_state.db_name}"
        engine = create_engine(connection_string)
    
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    # st.write(table_names)

    for tn in table_names:
        expander = st.expander(tn)
        expander.write(inspector.get_columns(tn))
    if db_mode == "postgres" or db_mode == "mysql":
        # st.write(inspector.get_columns("Order"))
        pass
    
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
    except Exception as e:
        st.error(f"Error: {e}")


if db_mode == "postgres":
    st.text_input("Database Host", key="db_host")
    st.number_input("Database Port", key="db_port", format='%u', value=5432)
    st.text_input("Database User", key="db_user")
    st.text_input("Database Password", type="password", key="db_password")
    st.text_input("Database Name", key="db_name")

elif db_mode == "sqlite":
    st.text_input("Database Path", key="db_path")

elif db_mode == "mysql":
    st.text_input("Database Host", key="db_host")
    st.number_input("Database Port", key="db_port", format='%u', value=3306)
    st.text_input("Database User", key="db_user")
    st.text_input("Database Password", type="password", key="db_password")
    st.text_input("Database Name", key="db_name")


# Add input field for timestamp to datetime conversion
st.write("Input timestamp:")
input_ts = st.number_input("Timestamp", min_value=0, max_value=9007199254740990, value=1643723401643, step=1)
st.write("Output datetime:")
output_datetime = pd.to_datetime(input_ts, unit=get_minimal_timestamp_format(input_ts))
st.write(output_datetime)

# Add output field for datetime to timestamp conversion
st.write("Input datetime:")
input_datetime = st.date_input("Date")
st.write("Output timestamp (in nanoseconds):")
output_timestamp = int(time.mktime(input_datetime.timetuple()) * 1e9)
st.write(output_timestamp)
