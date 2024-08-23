#!/usr/bin/env python3

"""Guardar la tabla de la consulta sql en un archivo."""

from google.cloud import bigquery
import pandas as pd

# pyright: reportAssignmentType = false

SQL = """
SELECT
    *
FROM
    dsongcp.trainday
"""

# Descarga y transforma la tabla
client: bigquery.Client = bigquery.Client()
df: pd.DataFrame = client.query(SQL).to_dataframe(date_dtype=pd.StringDtype())

# Guarda la tabla
df.to_csv(path_or_buf="../trainday.csv", index=False)
