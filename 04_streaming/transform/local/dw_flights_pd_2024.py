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
# Con esta consulta de las 6_000_000 de filas el proceso es eliminado
# SQL = """
# SELECT
#     *
# FROM bigquery-manu-407202.dsongcp.flights
# """
df: pd.DataFrame = client.query_and_wait(SQL).to_dataframe()
df["FL_DATE"] = pd.to_datetime(df["FL_DATE"])
df["FL_DATE"] = df["FL_DATE"].dt.strftime('%Y-%m-%d')

# Transformar columnas no numericas a string
not_string_cols = ['FL_DATE', 'DEP_DELAY', 'TAXI_OUT',
                'TAXI_IN', 'ARR_DELAY', 'CANCELLED', 'DIVERTED']
string_cols = [col for col in df.columns if col not in not_string_cols]
for col in string_cols:
    df[col] = df[col].astype(str)

# Completar con ceros a la izquierda las columnas con formato "hhmm"
for col in ["CRS_DEP_TIME", "DEP_TIME", "WHEELS_OFF", "WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
    df[col] = df[col].str.zfill(4)

# Eliminamos los vuelos cancelados y desviados con fines de desarrollo
df = df.loc[~df["DIVERTED"] & ~df["CANCELLED"]]


json = df.to_json(
    # "flights_2024.json", # No sirve se muere en esta parte al escribir
    "flights_sample_2024.json",
    orient="records",
    lines=True,
)
