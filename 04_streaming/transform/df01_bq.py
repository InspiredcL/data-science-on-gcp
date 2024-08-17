#!/usr/bin/env python3


"""df01 desde bq"""


import apache_beam as beam
from apache_beam.pipeline import Pipeline
from apache_beam.io.gcp.internal.clients import bigquery

# pyright: reportPrivateImportUsage=false


# Source
airports_table = bigquery.TableReference(
    projectId="bigquery-manu-407202",
    datasetId="dsongcp",
    tableId="airports",
)

if __name__ == "__main__":
    with Pipeline("DirectRunner") as pipeline:
        # Leer datos desde BigQuery
        read_airports = (
            pipeline
            | "airports:read"
            >> beam.io.ReadFromBigQuery(
                method=beam.io.ReadFromBigQuery.Method.DIRECT_READ,
                table=airports_table,
            )
            | beam.Map(
                lambda fields: (
                    (
                        fields["AIRPORT_SEQ_ID"],
                        (fields["LATITUDE"], fields["LONGITUDE"]),
                    ),
                )
            )
        )
        # Transformar datos
        transformed_airports_data = (
            read_airports
            | beam.io.WriteToText("df01_bq_extracted_airports")
        )
