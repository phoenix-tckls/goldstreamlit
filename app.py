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

df_high_low = conn.query(
    """
    select * from gold.processed.ohlc_calcs order by datetime desc;
    """
)

st.subheader(f'Forex Factory Events with candlestick movement calculations within 60 minutes after event', divider='blue')
st.caption(f'Forex Factory Data: until {str(df_ff["DATETIME"][0])}')
st.caption(f'Candlestick Data: until {str(df_high_low["DATETIME"][0])}')

df_ff_candlestick = df_ff.merge(df_high_low)
query = st.text_input("Enter filter of Forex Factory data", "")
mask = df_ff_candlestick.map(lambda x: query.lower() in str(x).lower()).any(axis=1)
df_ff_candlestick = df_ff_candlestick[mask]
st.dataframe(df_ff_candlestick)

def get_additional_calcs(event, dataframe, year_aggcalcs):
    dataframe_masked = dataframe[dataframe["DESCRIPTION"] == event]
    dataframe_masked = dataframe_masked[dataframe_masked["DATETIME"].dt.year >= int(year_aggcalcs)]
    bins = [0, 5, 10, 15, 20, 25, 100]
    categories = ['(0, 5]','(5, 10]','(10, 15]','(15, 20]','(20, 25]','(25, 100]']
    dataframe_masked['LARGEST_DIFF_FROM_OPEN_BINNED'] = pd.cut(dataframe_masked['LARGEST_DIFF_FROM_OPEN'], bins).astype(str)
    dataframe_masked['LARGEST_DIFF_FROM_OPEN_BINNED'] = pd.Categorical(
        dataframe_masked['LARGEST_DIFF_FROM_OPEN_BINNED'], 
        categories, 
        ordered=True
    )
    dataframe_masked['MAX_HIGH_MIN_LOW_DIFF_BINNED'] = pd.cut(dataframe_masked['MAX_HIGH_MIN_LOW_DIFF'], bins).astype(str)
    dataframe_masked['MAX_HIGH_MIN_LOW_DIFF_BINNED'] = pd.Categorical(
        dataframe_masked['MAX_HIGH_MIN_LOW_DIFF_BINNED'], 
        categories, 
        ordered=True
    )
    count_of_event = len(dataframe_masked)
    try:
        pctg_of_event_largest_diff_open_15 = len(dataframe_masked[dataframe_masked["LARGEST_DIFF_FROM_OPEN"] > 15]) / count_of_event
    except:
        pctg_of_event_largest_diff_open_15 = 0
    try:
        pctg_of_event_max_high_min_low_diff = len(dataframe_masked[dataframe_masked["MAX_HIGH_MIN_LOW_DIFF"] > 15]) / count_of_event
    except:
        pctg_of_event_max_high_min_low_diff = 0
    additional_calcs = [
        dataframe_masked["LARGEST_DIFF_FROM_OPEN"].mean(),
        dataframe_masked["LARGEST_DIFF_FROM_OPEN"].max(),
        dataframe_masked["MAX_HIGH_MIN_LOW_DIFF"].mean(),
        dataframe_masked["MAX_HIGH_MIN_LOW_DIFF"].max(),
        count_of_event,
        pctg_of_event_largest_diff_open_15,
        pctg_of_event_max_high_min_low_diff,
        dataframe_masked.groupby('LARGEST_DIFF_FROM_OPEN_BINNED').size().tolist(),
        dataframe_masked.groupby('MAX_HIGH_MIN_LOW_DIFF_BINNED').size().tolist()
    ]
    return additional_calcs


st.subheader('Aggregate calculations for each Forex Factory event', divider='blue')
year_aggcalcs = st.text_input("Enter year of candlestick data to calculate aggregates from", '2012')

all_unique_events_additional_calcs = []
for event in df_ff_candlestick["DESCRIPTION"].unique():
     all_unique_events_additional_calcs.append([event] + get_additional_calcs(event, df_ff_candlestick, year_aggcalcs))
all_unique_events_additional_calcs = pd.DataFrame(all_unique_events_additional_calcs)
all_unique_events_additional_calcs.columns = [
    'DESCRIPTION',
    'LARGEST_DIFF_FROM_OPEN_MEAN',
    'LARGEST_DIFF_FROM_OPEN_MAX',
    'MAX_HIGH_MIN_LOW_DIFF_MEAN',
    'MAX_HIGH_MIN_LOW_DIFF_MAX',
    'COUNT_OF_EVENT',
    'PCTG_OF_EVENT_LARGEST_DIFF_FROM_OPEN_>15',
    'PCTG_OF_EVENT_MAX_HIGH_MIN_LOW_DIFF_>15',
    'LARGEST_DIFF_FROM_OPEN_DISTRIBUTION_BUCKETS_OF_5',
    'MAX_HIGH_MIN_LOW_DIFF_DISTRIBUTION_BUCKETS_OF_5'
]
all_unique_events_additional_calcs['PCTG_OF_EVENT_LARGEST_DIFF_FROM_OPEN_>15'] = all_unique_events_additional_calcs['PCTG_OF_EVENT_LARGEST_DIFF_FROM_OPEN_>15'].map("{:,.3f}%".format)
all_unique_events_additional_calcs['PCTG_OF_EVENT_MAX_HIGH_MIN_LOW_DIFF_>15'] = all_unique_events_additional_calcs['PCTG_OF_EVENT_MAX_HIGH_MIN_LOW_DIFF_>15'].map("{:,.3f}%".format)
mask = all_unique_events_additional_calcs.map(lambda x: query.lower() in str(x).lower()).any(axis=1)
all_unique_events_additional_calcs = all_unique_events_additional_calcs[mask]
st.data_editor(
    all_unique_events_additional_calcs,
    column_config={
        'LARGEST_DIFF_FROM_OPEN_DISTRIBUTION_BUCKETS_OF_5': st.column_config.BarChartColumn(
            "LARGEST_DIFF_FROM_OPEN_DISTRIBUTION_BUCKETS_OF_5"
        ),
        'MAX_HIGH_MIN_LOW_DIFF_DISTRIBUTION_BUCKETS_OF_5': st.column_config.BarChartColumn(
            "MAX_HIGH_MIN_LOW_DIFF_DISTRIBUTION_BUCKETS_OF_5"
        ),
    }
)




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

