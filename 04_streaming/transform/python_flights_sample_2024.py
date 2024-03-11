#!/usr/bin/env python3


"""
_summary_

"""

from google.cloud import bigquery
import pandas as pd

client = bigquery.Client()
SQL = """
SELECT
    *
FROM bigquery-manu-407202.dsongcp.flights_sample
"""
df: pd.DataFrame = client.query_and_wait(SQL).to_dataframe()
df["FL_DATE"] = pd.to_datetime(df["FL_DATE"])
df["FL_DATE"] = df["FL_DATE"].dt.strftime('%Y-%m-%d')
json = df.to_json(
    "flights_sample_2024.json",
    orient="records",
    lines=True,
    date_format="iso"
)
