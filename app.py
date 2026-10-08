import os

import pandas as pd
import psycopg
import streamlit as st

st.set_page_config(page_title="GH Pulse", layout="wide")

DB_URL = os.environ.get("DATABASE_URL") or st.secrets.get("DATABASE_URL", "")


@st.cache_data(ttl=300)
def load() -> pd.DataFrame:
    if not DB_URL:
        return pd.DataFrame()
    try:
        with psycopg.connect(DB_URL) as con:
            return pd.read_sql(
                "SELECT date, repo_name, push_count, pr_count, comment_count, "
                "unique_contributors FROM repo_daily_metrics ORDER BY date",
                con,
            )
    except psycopg.errors.UndefinedTable:
        return pd.DataFrame()


st.title("GH Pulse — GitHub activity metrics")

df = load()
if df.empty:
    st.warning("No data. Set DATABASE_URL and run the pipeline to populate repo_daily_metrics.")
    st.stop()

df["total"] = df.push_count + df.pr_count + df.comment_count
df["date"] = pd.to_datetime(df["date"])

ranked = df.groupby("repo_name")["total"].sum().sort_values(ascending=False)
min_date, max_date = df.date.min().date(), df.date.max().date()

st.sidebar.header("Filters")
picked = st.sidebar.multiselect(
    "Repos", ranked.head(300).index.tolist(), default=ranked.head(15).index.tolist()
)
picked_range = st.sidebar.date_input(
    "Date range", value=(min_date, max_date), min_value=min_date, max_value=max_date
)
if not isinstance(picked_range, tuple) or len(picked_range) != 2:
    st.stop()
start, end = picked_range
metric = st.sidebar.selectbox(
    "Rank top repos by", ["total", "push_count", "pr_count", "comment_count", "unique_contributors"]
)
top_n = st.sidebar.slider("How many top repos", 5, 50, 15)

view = df[df.date.dt.date.between(start, end)]
if picked:
    view = view[view.repo_name.isin(picked)]

if view.empty:
    st.warning("No rows match the current filters.")
    st.stop()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Repos", view.repo_name.nunique())
c2.metric("Days", view.date.nunique())
c3.metric("Pushes", int(view.push_count.sum()))
c4.metric("PRs", int(view.pr_count.sum()))

st.subheader(f"Top repos by {metric}")
top = view.groupby("repo_name")[metric].sum().sort_values(ascending=False).head(top_n)
st.bar_chart(top)

st.subheader("Daily activity trend")
st.line_chart(view.groupby("date")[["push_count", "pr_count", "comment_count"]].sum())

st.subheader("Raw metrics")
st.dataframe(view, use_container_width=True)
