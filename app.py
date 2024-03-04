import streamlit as st
import plotly.graph_objects as go
from datetime import datetime
import pandas as pd
import numpy as np

# Initialize connection.
conn = st.connection("snowflake")

df_ff = conn.query(
    """
    select * from forexfactory order by datetime desc;
    """, 
    ttl=600
)

query = st.text_input("Filter dataframe")

st.dataframe(df_ff)

if query:
    mask = df_ff.map(lambda x: query.lower() in str(x).lower()).any(axis=1)
    df_ff = df_ff[mask]

    dates_result = df_ff['DATETIME'].astype(str).tolist()
    minute_window_input = st.text_input("Adjust window for minutes ahead (default = 60min)")

    def get_movement_with_timewindow(dates_result, minute_window_input):
        all_rows = []
        for d in dates_result:
            df_high_low = conn.query(
                f"""
                    select 
                        max(high) as max_high_in_{minute_window_input}_min,
                        min(low) as min_low_in_{minute_window_input}_min
                    from (
                        select * from gold.processed.ohlc 
                        where datetime > '{d}'
                        order by datetime 
                        limit {minute_window_input}
                    )
                """, 
                ttl=600
            )

            df_open = conn.query(
                f"""
                    select 
                        datetime, 
                        open as open_price_at_timestamp
                    from gold.processed.ohlc 
                    where datetime = '{d}'
                """, 
                ttl=600
            )

            data = pd.concat([df_open,df_high_low], axis=1).iloc[0].tolist()
            all_rows.append(data)

        df = pd.DataFrame(all_rows, columns=['DATETIME','OPEN_PRICE_AT_TIMESTAMP',f'MAX_HIGH_IN_{minute_window_input}_MIN',f'MIN_LOW_IN_{minute_window_input}_MIN'])
        df["open-high"] = abs(df['OPEN_PRICE_AT_TIMESTAMP'] - df[f'MAX_HIGH_IN_{minute_window_input}_MIN'])
        df["open-low"] = abs(df['OPEN_PRICE_AT_TIMESTAMP'] - df[f'MIN_LOW_IN_{minute_window_input}_MIN'])
        df["LARGEST_DIFF"] = np.where(df["open-high"] > df["open-low"], df["open-high"], df["open-low"])
        df["LARGEST_DIFF_CLASS"] = np.where(df["open-high"] > df["open-low"], "High", "Low")
        df = df.drop(["open-high","open-low"], axis=1)
        df = df[df['DATETIME'].notna()]

        return df

    try:
        df = df_ff[["DATETIME","DESCRIPTION","ACTUAL","FORECAST","PREVIOUS"]].merge(get_movement_with_timewindow(dates_result, minute_window_input))
    except:
        df = df_ff[["DATETIME","DESCRIPTION","ACTUAL","FORECAST","PREVIOUS"]].merge(get_movement_with_timewindow(dates_result, 60))
    
    st.dataframe(df)



dates_for_ohlc_1 = st.text_input("Input start date range for OHLC [YYYY-MM-DD]")
dates_for_ohlc_2 = st.text_input("Input end date range for OHLC [YYYY-MM-DD]")

try:
    df_xau_usd = conn.query(f"select * from ohlc where date(datetime) >= '{dates_for_ohlc_1}' and date(datetime) <= '{dates_for_ohlc_2}'", ttl=600)
except:
    df_xau_usd = conn.query(f"select * from ohlc order by datetime desc limit 50", ttl=600)

fig = go.Figure()
fig.add_trace(
    go.Candlestick(
        x=df_xau_usd["DATETIME"], 
        open=df_xau_usd["OPEN"], 
        high=df_xau_usd["HIGH"], 
        low=df_xau_usd["LOW"], 
        close=df_xau_usd["CLOSE"]
    )
)
st.plotly_chart(fig)
