#!/usr/bin/env python3

# pyright: reportOptionalMemberAccess=false

"""Crea una tabla desde una consulta SQL."""

from google.cloud import bigquery

PROJECT = "bigquery-manu-407202"
DATASET_ID = "dsongcp"
TABLE_ID = "trainday"
client = bigquery.Client(project=PROJECT)

SQL = f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DATASET_ID}.{TABLE_ID}`
OPTIONS(
    friendly_name="train_day_table",
    description="Una tabla que tiene las fechas elegidas para entrenamiento",
    labels=[("train", "development")]
) AS
SELECT
    FL_DATE,
    IF(
        ABS(MOD(FARM_FINGERPRINT(CAST(FL_DATE AS STRING)), 100)) < 70,
        'True',
        'False'
    ) AS is_train_day
FROM
    (
        SELECT
            DISTINCT(FL_DATE) AS FL_DATE
        FROM
            dsongcp.flights_tzcorr
    )
ORDER BY
    FL_DATE
"""

job: bigquery.QueryJob = client.query(SQL)  # API request.
job.result()  # Comienza el trabajo y espera a que la consulta finalice.

print(
    f"Nueva tabla creada '{job.destination.project}.{job.destination.dataset_id}.{job.destination.table_id}'"
)
