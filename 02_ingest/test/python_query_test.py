#!/usr/bin/env python3

'''
Query de ejemplo para ejecutar con la API de python de BigQuery
'''

from google.cloud import bigquery

# Construye un objeto BigQuery client.
client = bigquery.Client()

# Definimos la Query (1er ejemplo).
# QUERY = """
#     SELECT name, SUM(number) as total_people
#     FROM `bigquery-public-data.usa_names.usa_1910_2013`
#     WHERE state = 'TX'
#     GROUP BY name, state
#     ORDER BY total_people DESC
#     LIMIT 20
# """

# Definimos la Query (2do ejemplo).
# QUERY = (
#     'SELECT *'
#     'FROM `bigquery-public-data.usa_names.usa_1910_2013`'
#     'LIMIT 5')

# Definimos la Query de nuestra vista (3er ejemplo).
QUERY = """
    SELECT
        COUNT(*) as true_positives
    FROM dsongcp.local_flights_raw
    WHERE DepDelay < 15 AND ArrDelay <15;
    """

# Ejecutamos la consulta del string (solicitud a la API)
query_job = client.query(QUERY)
rows = query_job.result()  # Espera para guardar los resultados

# Imprimimos el resultado 1er o 2do ejemplo
print("Los datos de la consulta:")
for row in rows:
    # Row values can be accessed by field name or index.
    # print(f"\tname = {row[0]}, count = {row['total_people']}") # 1er ejemplo
    # print(f"\tElemento Row del iterable: {row}")
    # print(f"\tColumna 0 del iterable: {row[0]}")
    print(f"TP: {row.true_positives}")
