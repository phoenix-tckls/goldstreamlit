import streamlit as st
import plotly.express as px
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

st.subheader(f'Forex Factory Data (until {str(df_ff["DATETIME"][0])})', divider='blue')
st.dataframe(df_ff)


st.subheader(f'Candlestick calcs upon Forex Factory data', divider='blue')
query = st.text_input("Enter filter of Forex Factory data")

if query:
    mask = df_ff.map(lambda x: query.lower() in str(x).lower()).any(axis=1)
    df_ff = df_ff[mask]

    dates_result = df_ff['DATETIME'].astype(str).tolist()
    # minute_window_input = st.text_input("Adjust window for minutes ahead (default = 60min)")
    minute_window_input = 60

    def get_movement_with_timewindow(dates_result, minute_window_input=60):
        all_rows = []
        for d in dates_result:
            df_high_low = conn.query(
                f"""
                    select 
                        max(high) as max_high_in_{minute_window_input}_min,
                        min(low) as min_low_in_{minute_window_input}_min
                    from (
                        select * from gold.processed.ohlc 
                        where datetime >= '{d}'
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
        df["LARGEST_DIFF_FROM_OPEN"] = np.where(df["open-high"] > df["open-low"], df["open-high"], df["open-low"])
        df["LARGEST_DIFF_FROM_OPEN_CLASS"] = np.where(df["open-high"] > df["open-low"], "High", "Low")
        df["MAX_HIGH_MIN_LOW_DIFF"] = abs(df[f'MAX_HIGH_IN_{minute_window_input}_MIN'] - df[f'MIN_LOW_IN_{minute_window_input}_MIN'])
        df = df.drop(["open-high","open-low"], axis=1)
        df = df[df['DATETIME'].notna()]

        return df

    try:
        df_ff_candlestick = df_ff[["DATETIME","IMPACT","DESCRIPTION","ACTUAL","FORECAST","PREVIOUS"]].merge(get_movement_with_timewindow(dates_result, minute_window_input))
    except:
        df_ff_candlestick = df_ff[["DATETIME","IMPACT","DESCRIPTION","ACTUAL","FORECAST","PREVIOUS"]].merge(get_movement_with_timewindow(dates_result, 60))
    
    st.dataframe(df_ff_candlestick)

    st.text(f"Aggregation calcs for {query} event (from 2012 data onwards):")
    st.write("Average Largest Diff from open price:",df_ff_candlestick[df_ff_candlestick["DATETIME"].dt.year >= 2012]["LARGEST_DIFF_FROM_OPEN"].mean())
    st.write("Maxiumum Largest Diff from open price:",df_ff_candlestick[df_ff_candlestick["DATETIME"].dt.year >= 2012]["LARGEST_DIFF_FROM_OPEN"].max())
    st.write("Average 'Max High-Min Low' Diff:",df_ff_candlestick[df_ff_candlestick["DATETIME"].dt.year >= 2012]["MAX_HIGH_MIN_LOW_DIFF"].mean())
    st.write("Maximum 'Max High-Min Low' Diff:",df_ff_candlestick[df_ff_candlestick["DATETIME"].dt.year >= 2012]["MAX_HIGH_MIN_LOW_DIFF"].max())





max_candlestick = conn.query(f"select max(datetime) as max_datetime from gold.processed.ohlc", ttl=600)["MAX_DATETIME"][0]
st.subheader(f'Candlestick data (until {max_candlestick})', divider='blue')
dates_for_ohlc_1 = st.text_input("Input start date range for candlestick data [YYYY-MM-DD]")
dates_for_ohlc_2 = st.text_input("Input end date range for candlestick date [YYYY-MM-DD]")

try:
    df_xau_usd = conn.query(f"select * from gold.processed.ohlc where date(datetime) >= '{dates_for_ohlc_1}' and date(datetime) <= '{dates_for_ohlc_2}'", ttl=600)
except:
    df_xau_usd = conn.query(f"select * from gold.processed.ohlc order by datetime desc limit 50", ttl=600)

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







st.subheader('Swing over time (per 60 minutes)', divider='blue')
df_ohlc = conn.query(
    """
        WITH temp as (
            SELECT datetime, open as open_price_at_timestamp, high, low
            FROM gold.processed.ohlc
            ORDER BY 1
        ),
        min_max_calc as (
            SELECT 
                datetime, 
                open_price_at_timestamp,
                MAX(high) OVER (ORDER BY datetime ROWS BETWEEN CURRENT ROW AND 59 FOLLOWING) AS max_high_in_60_min,
                MIN(low) OVER (ORDER BY datetime ROWS BETWEEN CURRENT ROW AND 59 FOLLOWING) AS min_low_in_60_min
            FROM temp
            ORDER BY datetime
        )
        SELECT
            datetime as hourly_timestamp,
            ROUND(ABS(open_price_at_timestamp - max_high_in_60_min), 3) AS diff_open_maxhigh,
            ROUND(ABS(open_price_at_timestamp - min_low_in_60_min), 3) AS diff_open_minlow,
            CASE
                WHEN diff_open_maxhigh > diff_open_minlow
                THEN diff_open_maxhigh
                ELSE diff_open_minlow
            END AS largest_diff_from_open,
            CASE
                WHEN diff_open_maxhigh > diff_open_minlow
                THEN 'High'
                ELSE 'Low'
            END AS largest_diff_from_open_class,
            ROUND(max_high_in_60_min - min_low_in_60_min, 3) AS max_high_min_low_diff 
        FROM min_max_calc
        WHERE MINUTE(datetime) = 0
    """, 
    ttl=600
)


fig2 = px.line(df_ohlc, x="HOURLY_TIMESTAMP", y="MAX_HIGH_MIN_LOW_DIFF", title='Difference b/w max high & min low')
st.plotly_chart(fig2)

fig3 = px.line(df_ohlc, x="HOURLY_TIMESTAMP", y="LARGEST_DIFF_FROM_OPEN", title='Largest Difference from open (could be max high or min low)')
st.plotly_chart(fig3)

