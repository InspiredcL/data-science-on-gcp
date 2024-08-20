#!/usr/bin/env python3

"""Guardar la tabla de la consulta sql en un archivo."""

from google.cloud import bigquery
import pandas as pd

# pyright: reportAssignmentType = false

SQL = """
SELECT
    *
FROM bigquery-manu-407202.dsongcp.flights
"""

# Descarga y transforma la tabla
client = bigquery.Client()
df = client.query(SQL).to_dataframe(date_dtype=pd.StringDtype())

# Guarda la tabla en trozos
CHUNK_SIZE = 300000
num_chunks = len(df) // CHUNK_SIZE + 1
for i in range(0, len(df), CHUNK_SIZE):
    chunk = df[i:i + CHUNK_SIZE]
    chunk.to_json(
        f"flights/chunks/flights_{str(i // CHUNK_SIZE).zfill(5)}-of-{str(num_chunks).zfill(5)}.jsonl",
        orient="records",
        lines=True,
    )
