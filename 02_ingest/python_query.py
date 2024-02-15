'''
_summary_
'''

from google.cloud import bigquery

client = bigquery.Client()

# Perform a query.
# QUERY = (
#     'SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` '
#     'WHERE state = "TX" '
#     'LIMIT 100')

QUERY = """
SELECT
    COUNT(*) as true_positives
FROM dsongcp.flights
WHERE dep_delay < 15 AND arr_delay <15;
"""
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row.true_positives)


