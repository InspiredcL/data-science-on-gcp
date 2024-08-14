#!/usr/bin/env python3

# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
Ejecuta un pipeline de Apache Beam en la nube para procesar datos de
vuelos y generar eventos simulados.
"""

import logging
import json
import datetime
import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
import timezonefinder
import pytz


# pylint: disable=expression-not-assigned
# pylint: disable=unnecessary-lambda
# pyright: reportUnusedImport=false
# pyright: reportPrivateImportUsage=false
# pyright: reportUnusedExpression=false
# pyright: reportOptionalMemberAccess=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportGeneralTypeIssues =false

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


def addtimezone(lat, lon):
    """
    Agrega la zona horaria correspondiente a las coordenadas proporcionadas.
    """

    try:
        # Crear una instancia de TimezoneFinder, para reutilizar
        tf = timezonefinder.TimezoneFinder()
        # Convertir las coordenadas a números de punto flotante
        lat = float(lat)
        lon = float(lon)
        # Devolver las coordenadas y la zona horaria correspondiente
        return lat, lon, tf.timezone_at(lng=lon, lat=lat)
    except ValueError:
        # Manejo de excepción en caso de error de valor
        return lat, lon, "TIMEZONE"  # Encabezado


def as_utc(date, hhmm, tzone):
    """Convierte una fecha y hora en formato UTC."""

    try:
        # Verificar si se proporcionó la hora y la zona horaria
        if len(hhmm) > 0 and tzone is not None:

            # Crear un objeto de zona horaria local
            loc_tz = pytz.timezone(tzone)
            # Crear un objeto de fecha y hora local con la fecha proporcionada
            loc_dt = loc_tz.localize(
                datetime.datetime.strptime(date, "%Y-%m-%d"), is_dst=False
            )
            # Agregar la diferencia de horas y minutos proporcionada a la fecha y hora local
            loc_dt += datetime.timedelta(
                hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
            # Convertir la fecha y hora local a UTC
            utc_dt = loc_dt.astimezone(pytz.utc)
            # Devolver la fecha y hora en formato UTC y el desplazamiento de la zona horaria
            return (
                utc_dt.strftime(DATETIME_FORMAT),
                loc_dt.utcoffset().total_seconds(),
            )
        else:
            # Si no se proporciona la hora o la zona horaria, devolver valores predeterminados
            return "", 0  # Una cadena vacía corresponde a vuelos cancelados
    except ValueError as e:
        # Manejar excepciones y registrar detalles
        logging.exception("%s %s %s, ValueError: %s", date, hhmm, tzone, e)
        # raise e


def add_24h_if_before(arrtime, deptime):
    """Agrega 24 horas a la hora de llegada"""

    if len(arrtime) > 0 and len(deptime) > 0 and arrtime < deptime:
        adt = datetime.datetime.strptime(arrtime, DATETIME_FORMAT)
        adt += datetime.timedelta(hours=24)
        return adt.strftime(DATETIME_FORMAT)
    else:
        return arrtime


def airport_timezone(airport_id, airport_timezones):
    """Busca la zona horaria asociada a un aeropuerto determinado."""

    if airport_id in airport_timezones:
        return airport_timezones[airport_id]
    else:
        # Eliminamos u ya que los strings en python son unicode por defecto
        return "37.41", "-92.35", "America/Chicago"


def tz_correct(fields, airport_timezones):
    """Realiza un ajuste de zonas horarias."""

    # Compatibilidad con JSON y BigQuery
    fields["FL_DATE"] = fields["FL_DATE"].strftime("%Y-%m-%d")

    # Convertir a UTC
    dep_airport_id = fields["ORIGIN_AIRPORT_SEQ_ID"]
    arr_airport_id = fields["DEST_AIRPORT_SEQ_ID"]
    fields["DEP_AIRPORT_LAT"], fields["DEP_AIRPORT_LON"], dep_timezone = (
        airport_timezone(dep_airport_id, airport_timezones)
    )
    fields["ARR_AIRPORT_LAT"], fields["ARR_AIRPORT_LON"], arr_timezone = (
        airport_timezone(arr_airport_id, airport_timezones)
    )

    for f in ["CRS_DEP_TIME", "DEP_TIME", "WHEELS_OFF"]:
        fields[f], deptz = as_utc(fields["FL_DATE"], fields[f], dep_timezone)
    for f in ["WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
        fields[f], arrtz = as_utc(fields["FL_DATE"], fields[f], arr_timezone)

    for f in ["WHEELS_OFF", "WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
        fields[f] = add_24h_if_before(fields[f], fields["DEP_TIME"])

    fields["DEP_AIRPORT_TZOFFSET"] = deptz
    fields["ARR_AIRPORT_TZOFFSET"] = arrtz
    yield fields


def get_next_event(fields):
    """Determina el siguiente evento de un vuelo"""

    if len(fields["DEP_TIME"]) > 0:
        event = dict(fields)  # copy
        event["EVENT_TYPE"] = "departed"
        event["EVENT_TIME"] = fields["DEP_TIME"]
        for f in [
            "TAXI_OUT",
            "WHEELS_OFF",
            "WHEELS_ON",
            "TAXI_IN",
            "ARR_TIME",
            "ARR_DELAY",
            "DISTANCE",
        ]:
            event.pop(f, None)  # not knowable at departure time
        yield event
    if len(fields["WHEELS_OFF"]) > 0:
        event = dict(fields)  # copy
        event["EVENT_TYPE"] = "wheelsoff"
        event["EVENT_TIME"] = fields["WHEELS_OFF"]
        for f in ["WHEELS_ON", "TAXI_IN", "ARR_TIME", "ARR_DELAY", "DISTANCE"]:
            event.pop(f, None)  # not knowable at departure time
        yield event
    if len(fields["ARR_TIME"]) > 0:
        event = dict(fields)
        event["EVENT_TYPE"] = "arrived"
        event["EVENT_TIME"] = fields["ARR_TIME"]
        yield event


def create_event_row(fields):
    """Crea una fila de evento para optimizar las consultas en bigquery."""

    featdict = dict(fields)  # copy
    featdict["EVENT_DATA"] = json.dumps(fields)
    return featdict


def run(project, region):
    """Ejecuta un pipeline de Apache Beam para procesar datos de vuelos"""

    argv = [
        f"--project={project}",
        f"--region={region}",
        "--runner=DirectRunner",
    ]

    # Source
    airports_table = bigquery.TableReference(
        projectId="bigquery-manu-407202", datasetId="dsongcp", tableId="airports"
    )
    flights_query = "SELECT * FROM dsongcp.flights_sample"
    # Sink
    flights_output = "df06_local_all_flights"
    events_output = "df06_local_all_events"

    with beam.Pipeline(argv=argv) as pipeline:
        # Source 1
        airports = (
            pipeline
            | "airports:read"
            >> beam.io.ReadFromBigQuery(
                method=beam.io.ReadFromBigQuery.Method.DIRECT_READ, table=airports_table
            )
            | "airports:onlyUSA" >> beam.Filter(
                lambda field: field["AIRPORT_COUNTRY_NAME"] == "United States")
            | "airports:tz"
            >> beam.Map(lambda fields: (
                fields["AIRPORT_SEQ_ID"],
                addtimezone(fields["LATITUDE"], fields["LONGITUDE"]))
            )
        )

        flights = (
            pipeline
            | "flights:read"
            >> beam.io.ReadFromBigQuery(
                method="DIRECT_READ",
                query=flights_query,
                use_standard_sql=True,
            )
            | "flights:tzcorr" >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
        )

        # Solo a local
        (
            flights
            | "flights:tostring" >> beam.Map(lambda fields: json.dumps(fields))
            | "flights:gcsout" >> beam.io.textio.WriteToText(flights_output)
        )

        events = flights | beam.FlatMap(get_next_event)
        (events
         | 'events:tostring' >> beam.Map(lambda fields: json.dumps(fields))
         | 'events:out' >> beam.io.textio.WriteToText(events_output)
         )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Ejecuta el pipeline localmente")
    parser.add_argument("-p", "--project",
                        help="ID único de proyecto", required=True)
    parser.add_argument(
        "-r",
        "--region",
        help="Region para ejecutar el trabajo. Eligge la misma reggion que tu bucket.",
        required=True,
    )
    args = vars(parser.parse_args())
    print("Corrigiendo marcas de tiempo y escribiendo a un archivo local")
    logging.getLogger().setLevel(logging.INFO)
    run(project=args["project"], region=args["region"])
