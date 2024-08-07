#!/usr/bin/env python3


"""
Doc_string

    _extended_summary_
"""

from google.cloud import bigquery
from tabulate import tabulate  # pylint: disable=import-error


# Construct a BigQuery client object.
client = bigquery.Client()

QUERY = """
WITH contingency_table AS (
    SELECT 
        THRESH,
        COUNTIF(dep_delay < THRESH AND arr_delay < 15) AS true_positives,
        COUNTIF(dep_delay < THRESH AND arr_delay >= 15) AS false_positives,
        COUNTIF(dep_delay >= THRESH AND arr_delay < 15) AS false_negatives,
        COUNTIF(dep_delay >= THRESH AND arr_delay >= 15) AS true_negatives,
        COUNT(*) AS total
    FROM
        dsongcp.flights,
        UNNEST([5, 10, 11, 12, 13, 15, 20]) AS THRESH
    WHERE arr_delay IS NOT NULL AND dep_delay IS NOT NULL
    GROUP BY THRESH
)

SELECT
    (true_positives + true_negatives)/total AS accuracy,
    false_positives/(true_positives + false_positives) AS fpr,
    false_negatives/(false_negatives + true_negatives) AS fnr,
    *
FROM
    contingency_table
ORDER BY 
    accuracy ASC;

"""
row_iterator = client.query_and_wait(QUERY)  # Make an API request.

# print("The query data:", row_iterator)
# for row in row_iterator:
#     # Row values can be accessed by field name or index.
#     #print("name={}, count={}".format(row[0], row["total_people"]))
#     print(row)
# TODO arreglar contador de filas de la tabla

# Obtener los nombres de las columnas
column_names = [field.name for field in row_iterator.schema]
# Agregar el nombre de la columna "Fila"
column_names.insert(0, "Row")
print(column_names)

# Convertir el RowIterator a un diccionario de listas
table_data = {}
for i, row in enumerate(row_iterator, start=1):
    table_data["Row"] = [i]
    for column_name in column_names[1:]:
        if column_name not in table_data:
            table_data[column_name] = []
        table_data[column_name].append(row[column_name])

# Imprimir la tabla usando tabulate
print(tabulate(table_data, headers=column_names, tablefmt="pipe"))
