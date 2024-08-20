#!/usr/bin/env python3

"""df01 desde bq"""

import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery
import timezonefinder

# pylint: disable=unnecessary-lambda
# pyright: reportPrivateImportUsage=false
# pyright: reportAttributeAccessIssue=false


def addtimezone(lat: str, lon: str):
    """Agrega la zona horaria correspondiente"""

    try:
        tf = timezonefinder.TimezoneFinder()
        return lat, lon, tf.timezone_at(lng=float(lat), lat=float(lon))
    # Valores incorrectos o Nulos:
    except (ValueError, TypeError):
        return lat, lon, "TIMEZONE"  # header


def date_to_string(fila):
    "Json test"
    # json.loads(fila) Cuando se lee de archivo
    fila["AIRPORT_START_DATE"] = fila["AIRPORT_START_DATE"].strftime("%Y-%m-%d")
    # Si es aeropuerto antiguo
    if fila["AIRPORT_IS_LATEST"] == 0:
        fila["AIRPORT_THRU_DATE"] = fila["AIRPORT_THRU_DATE"].strftime(
            "%Y-%m-%d"
        )
    return fila


def select_fields(diccionario):
    "Test"
    # Selecciona las claves que deseas mantener
    claves_filtradas = {"AIRPORT_SEQ_ID", "Valor"}
    # Crea un nuevo diccionario solo con las claves seleccionadas
    return {clave: diccionario[clave] for clave in claves_filtradas}


# Options
beam_options = PipelineOptions(project="bigquery-manu-407202")
# Source (Table or Query)
airports_table = bigquery.TableReference(
    projectId="bigquery-manu-407202",
    datasetId="dsongcp",
    tableId="airports",
)
AIRPORTS_QUERY = "SELECT * FROM bigquery-manu-407202.dsongcp.airports LIMIT 5"

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    with beam.Pipeline(runner="DirectRunner", options=beam_options) as pipeline:
        # Leer datos desde BigQuery a una PCollection
        bq_read = pipeline | "airports:table_read" >> beam.io.ReadFromBigQuery(
            method=beam.io.ReadFromBigQuery.Method.DIRECT_READ,
            # method=beam.io.ReadFromBigQuery.Method.EXPORT,
            # table=airports_table,
            query=AIRPORTS_QUERY,
            use_standard_sql=True,
        )
        # Escribimos la PCollection a texto
        airports_no_format = (
            bq_read
            | "airports:no_format_text"
            >> beam.io.textio.WriteToText(
                "./airports_bq_files/airports_raw_input"
            )
        )
        # Escribimos la PCollection a texto formato JSON
        airports_json = (
            bq_read
            | "airports:date_to_string" >> beam.Map(date_to_string)
            | "airports:to_string" >> beam.Map(lambda row: json.dumps(row))
            | "airports:json_text"
            >> beam.io.WriteToText("./airports_bq_files/airports_json")
        )
        airports_filtered = (
            bq_read
            | "airports:USA"
            >> beam.Filter(
                lambda row: row["AIRPORT_COUNTRY_NAME"] == "United States"
            )
            | "airports:select_fields"
            >> beam.Map(
                lambda fields: (
                    fields["AIRPORT_SEQ_ID"],
                    addtimezone(fields["LATITUDE"], fields["LONGITUDE"]),
                )
            )
        )
        airports_filtered_no_format = (
            airports_filtered
            | "airports_filtered:no_format_text"
            >> beam.io.textio.WriteToText(
                "./airports_bq_files/airports_raw_input_filtered"
            )
        )
        airports_filtered_json = (
            airports_filtered
            | "airports_filtered:to_string"
            >> beam.Map(lambda row: json.dumps(row))
            | "airports_filtered:json_text"
            >> beam.io.WriteToText("./airports_bq_files/airports_json_filtered")
        )
# json.dumps(fila)
