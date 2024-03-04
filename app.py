import streamlit as st
import plotly.graph_objects as go
from datetime import datetime

# Initialize connection.
conn = st.connection("snowflake")

df_ff = conn.query(
    """
    select * from forexfactory order by datetime desc;
    """, 
    ttl=600)

query = st.text_input("Filter dataframe")

if query:
    mask = df_ff.map(lambda x: query.lower() in str(x).lower()).any(axis=1)
    df_ff = df_ff[mask]

st.data_editor(df_ff)

dates_for_ohlc_1 = st.text_input("Input start date range for OHLC [YYYY-MM-DD]")
dates_for_ohlc_2 = st.text_input("Input end date range for OHLC [YYYY-MM-DD]")

df_xau_usd = conn.query(f"select * from ohlc where date(datetime) >= '{dates_for_ohlc_1}' and date(datetime) <= '{dates_for_ohlc_2}';", ttl=600)

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


timestamp = st.text_input("Event timestamp: timestamp open vs. timestamp low / high price within + 1h")
open_original = conn.query(f"select open from ohlc where datetime = to_timestamp('{timestamp}');", ttl=600)["OPEN"][0]
highest_within_plus_1h = conn.query(f"SELECT datetime as minutes_after, max(high) as highest from ohlc where datetime > to_timestamp('{timestamp}')and datetime <= dateadd(hour,1,to_timestamp('{timestamp}')) group by 1 order by 2 desc;", ttl=600).iloc[0]
lowest_within_plus_1h = conn.query(f"SELECT datetime as minutes_after, min(low) as lowest from ohlc where datetime > to_timestamp('{timestamp}')and datetime <= dateadd(hour,1,to_timestamp('{timestamp}')) group by 1 order by 2;", ttl=600).iloc[0]
highest_price = highest_within_plus_1h['HIGHEST']
highest_minutes_after = (datetime.strptime(str(highest_within_plus_1h['MINUTES_AFTER']), '%Y-%m-%d %H:%M:%S') - datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S')).total_seconds() / 60.0
lowest_price = lowest_within_plus_1h['LOWEST']
lowest_minutes_after = (datetime.strptime(str(lowest_within_plus_1h['MINUTES_AFTER']), '%Y-%m-%d %H:%M:%S') - datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S')).total_seconds() / 60.0
st.write(f"Open amount (timestamp of event): {open_original}")
st.write(f"High amount (timestamp of event + {highest_minutes_after} minutes): {highest_price}")
st.write(f"Low amount (timestamp of event + {lowest_minutes_after} minutes): {lowest_price}")
if abs(open_original-highest_price) > abs(open_original-lowest_price):
    st.write(f"Biggest difference: {abs(round(open_original-highest_price, 3))} (high)")
elif abs(open_original-lowest_price) > abs(open_original-highest_price):
    st.write(f"Biggest difference: {abs(round(open_original-lowest_price, 3))} (low)")
else:
    st.write(f"Biggest difference: {abs(round(open_original-highest_price, 3))}")
