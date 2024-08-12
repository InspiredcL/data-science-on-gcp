#!/usr/bin/env python3

"""
_summary_
"""

import pandas as pd
import pandas_gbq

# pyright: reportAssignmentType = false

# Con esta consulta de las 6_000_000 de filas el proceso es eliminado
SQL = """
SELECT
    *
FROM bigquery-manu-407202.dsongcp.flights
"""

# Utilizamos read_gbq para leer la consulta directamente en un DataFrame
PROJECT_BQ = "bigquery-manu-407202"
df: pd.DataFrame = pandas_gbq.read_gbq(SQL, project_id=PROJECT_BQ)

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

# Convertir el DataFrame a JSON
json = df.to_json(
    "flights_2024.json",
    orient="records",
    lines=True,
)
