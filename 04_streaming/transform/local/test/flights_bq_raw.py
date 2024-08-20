#!/usr/bin/env python3

""" Desde bq"""

import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# pyright: reportPrivateImportUsage=false
# pyright: reportAttributeAccessIssue=false


def date_to_string(fila):
    "Json test"
    # json.loads(fila) Cuando se lee de archivo
    fila["FL_DATE"] = fila["FL_DATE"].strftime("%Y-%m-%d")
    return fila


# Options
beam_options = PipelineOptions(project="bigquery-manu-407202")
# Source
FLIGHTS_QUERY = "SELECT * FROM dsongcp.flights WHERE rand() < 0.001"
# Sink
FLIGHTS_OUTPUT = "flights_raw_bq_input"
FLIGHTS_OUTPUT_JSON = "flights_json_bq_input"


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    with beam.Pipeline(runner="DirectRunner", options=beam_options) as pipeline:
        # Leer datos desde BigQuery a una PCollection
        bq_read = pipeline | "flights:read" >> beam.io.ReadFromBigQuery(
            method=beam.io.ReadFromBigQuery.Method.DIRECT_READ,
            query=FLIGHTS_QUERY,
            use_standard_sql=True,
        )
        # Escribimos la PCollection a texto
        airports_no_format = (
            bq_read
            | "flights:no_format_text"
            >> beam.io.textio.WriteToText(FLIGHTS_OUTPUT)
        )
        # Escribimos la PCollection a texto formato JSON
        airports_json = (
            bq_read
            | "flights:date_to_string" >> beam.Map(date_to_string)
            | "flights:to_string" >> beam.Map(json.dumps)
            | "flights:json_text" >> beam.io.WriteToText(FLIGHTS_OUTPUT_JSON)
        )


# def select_fields(diccionario):
#     "Test"
#     # Selecciona las claves que deseas mantener
#     claves_filtradas = {"AIRPORT_SEQ_ID", "Valor"}
#     # Crea un nuevo diccionario solo con las claves seleccionadas
#     return {clave: diccionario[clave] for clave in claves_filtradas}
