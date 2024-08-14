#!/usr/bin/env python3


"""
_summary_

_extended_summary_

Raises:
    e: _description_

Returns:
    _description_

Yields:
    _description_
"""

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

import logging
import csv
import json
import apache_beam as beam


# pylint: disable=import-outside-toplevel
# pylint: disable=multiple-imports
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

    **Argumentos:**
    * `lat`: Latitud en grados decimales.
    * `lon`: Longitud en grados decimales.

    **Devuelve:**
    Una tupla con las coordenadas y la zona horaria correspondiente.

    **Excepción:**
    * `ValueError`: Si las coordenadas no son válidas.

    **Ejemplo:**
        addtimezone(-33.45, -70.66)
        (-33.45, -70.66, 'America/Santiago')

    **Documentación adicional:**
    * La función utiliza la librería `timezonefinder` para obtener la zona
    horaria correspondiente a las coordenadas proporcionadas.
    * La función maneja la excepción `ValueError` en caso de que las coordenadas no sean válidas.
    """

    try:
        import timezonefinder

        tf = timezonefinder.TimezoneFinder()
        lat = float(lat)
        lon = float(lon)
        return lat, lon, tf.timezone_at(lng=lon, lat=lat)
    except ValueError:
        return lat, lon, "TIMEZONE"  # header


def as_utc(date, hhmm, tzone):
    """
    Convierte una fecha y hora en formato UTC a la hora corregida para una zona horaria específica.

    **Argumentos:**

    * `date`: Fecha en formato `YYYY-MM-DD`.
    * `hhmm`: Hora en formato `HH:MM`.
    * `tzone`: Zona horaria en formato `TZ`.

    **Devuelve:**

    * `utc_dt`: Objeto de fecha y hora en formato `YYYY-MM-DDTHH:MM:SS+00:00`.
    * `tz_offset`: Desplazamiento de la zona horaria en segundos.

    **Excepción:**

    * `ValueError`: Si la fecha, la hora o la zona horaria no son válidas.
    """

    try:
        if len(hhmm) > 0 and tzone is not None:
            import datetime
            import pytz

            loc_tz = pytz.timezone(tzone)
            loc_dt = loc_tz.localize(
                datetime.datetime.strptime(date, "%Y-%m-%d"), is_dst=False
            )
            # Considera las horas 2400 y 0000
            loc_dt += datetime.timedelta(
                hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
            utc_dt = loc_dt.astimezone(pytz.utc)
            return utc_dt.strftime(DATETIME_FORMAT), loc_dt.utcoffset().total_seconds()
        else:
            return "", 0  # empty string corresponds to canceled flights
    except ValueError as e:
        logging.exception("%s %s %s, ValueError: %s", date, hhmm, tzone, e)
        # raise e


def add_24h_if_before(arrtime, deptime):
    """
    Agrega 24 horas a la hora de llegada (arrtime) si es anterior a la
    hora de salida (deptime).

    **Argumentos:**

    * `arrtime`: Hora de llegada en formato `YYYY-MM-DDTHH:MM:SS`.
    * `deptime`: Hora de salida en formato `YYYY-MM-DDTHH:MM:SS`.

    **Devuelve:**

    * `arrtime` si la hora de llegada es posterior o igual a la hora de salida.
    * `arrtime` más 24 horas si la hora de llegada es anterior a la hora
    de salida.

    **Excepción:**

    * `ValueError`: Si las horas de llegada o salida no están en formato válido.
    """

    import datetime

    if len(arrtime) > 0 and len(deptime) > 0 and arrtime < deptime:
        adt = datetime.datetime.strptime(arrtime, DATETIME_FORMAT)
        adt += datetime.timedelta(hours=24)
        return adt.strftime(DATETIME_FORMAT)
    else:
        return arrtime


def tz_correct(fields, airport_timezones):
    """
    Realiza un ajuste de zonas horarias para los campos de fecha y hora de un
    diccionario de datos de vuelo.

    **Argumentos:**

    * `fields`: Diccionario que contiene los datos de vuelo, incluyendo
    campos de fecha y hora.
    * `airport_timezones`: Diccionario que mapea los identificadores de
    aeropuerto a sus respectivas zonas horarias.

    **Devuelve:**

    * Un generador que produce un diccionario con los campos de fecha y hora ajustados a UTC.

    **Proceso:**

    1. Convierte la fecha de vuelo a una cadena en formato `YYYY-MM-DD`.
    2. Obtiene las zonas horarias de los aeropuertos de salida y llegada.
    3. Convierte los tiempos de salida a UTC.
    4. Convierte los tiempos de llegada a UTC.
    5. Corrige los tiempos de llegada que sean anteriores a los tiempos
    de salida, agregando 24 horas.
    6. Agrega los desplazamientos de zona horaria de los aeropuertos de
    salida y llegada al diccionario de datos.
    """

    # Compatibilidad con JSON y BigQuery
    fields["FL_DATE"] = fields["FL_DATE"].strftime("%Y-%m-%d")
    try:
        # Convertir a UTC
        dep_airport_id = fields["ORIGIN_AIRPORT_SEQ_ID"]
        arr_airport_id = fields["DEST_AIRPORT_SEQ_ID"]
        dep_timezone = airport_timezones[dep_airport_id][2]
        arr_timezone = airport_timezones[arr_airport_id][2]
        for f in ["CRS_DEP_TIME", "DEP_TIME", "WHEELS_OFF"]:
            fields[f], deptz = as_utc(
                fields["FL_DATE"], fields[f], dep_timezone)
        for f in ["WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
            fields[f], arrtz = as_utc(
                fields["FL_DATE"], fields[f], arr_timezone)

        # Si la llegada es antes que la salida
        for f in ["WHEELS_OFF", "WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
            fields[f] = add_24h_if_before(fields[f], fields["DEP_TIME"])

        fields["DEP_AIRPORT_LAT"] = airport_timezones[dep_airport_id][0]
        fields["DEP_AIRPORT_LON"] = airport_timezones[dep_airport_id][1]
        fields["DEP_AIRPORT_TZOFFSET"] = deptz
        fields["ARR_AIRPORT_LAT"] = airport_timezones[arr_airport_id][0]
        fields["ARR_AIRPORT_LON"] = airport_timezones[arr_airport_id][1]
        fields["ARR_AIRPORT_TZOFFSET"] = arrtz
        yield fields
    except KeyError:
        logging.exception(
            "Ignoring %s because airport is not known,error: %s", fields, KeyError
        )


def get_next_event(fields):
    """
    Determina el siguiente evento de un vuelo a partir de los campos de datos disponibles.

    **Argumentos:**

    * `fields`: Diccionario que contiene los datos de vuelo, incluyendo campos de fecha y hora.

    **Devuelve:**

    * Un generador que produce un diccionario con los siguientes elementos:
        - `EVENT_TYPE`: Tipo de evento ('departed', 'wheelsoff' o 'arrived').
        - `EVENT_TIME`: Hora del evento en formato UTC.
        - Otros campos de datos de vuelo relevantes.

    **Proceso:**

    1. Verifica la disponibilidad de los campos `DEP_TIME`, `WHEELS_OFF` y `ARR_TIME`.
    2. Si `DEP_TIME` está disponible, genera un evento de tipo "departed".
    3. Si `WHEELS_OFF` está disponible, genera un evento de tipo "wheelsoff".
    4. Si `ARR_TIME` está disponible, genera un evento de tipo "arrived".
    5. Elimina los campos que no son relevantes para el evento actual.
    """

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
    """
    Crea una fila de evento para ser utilizada en un formato tabular.

    **Argumentos:**

    * `fields`: Diccionario que contiene los datos del evento, incluyendo
    campos como `EVENT_TYPE`, `EVENT_TIME` y otros campos de datos del vuelo.

    **Devuelve:**

    * Un diccionario con los siguientes elementos:
        - Todos los campos del diccionario `fields` original.
        - Un campo adicional llamado `EVENT_DATA` que contiene los datos del evento en formato JSON.

    **Proceso:**

    1. Crea una copia del diccionario `fields`.
    2. Serializa los datos del evento en formato JSON y los almacena en el
    campo `EVENT_DATA` del nuevo diccionario.
    3. Devuelve el diccionario con los datos del evento serializados.
    """

    featdict = dict(fields)  # copy
    featdict["EVENT_DATA"] = json.dumps(fields)
    return featdict


def run(project, bucket):
    """
    Ejecuta el pipeline con los argumentos correspondientes
    """

    argv = [
        f"--project={project}",
        f"--staging_location=gs://{bucket}/flights/staging/",
        f"--temp_location=gs://{bucket}/flights/temp/",
        "--runner=DirectRunner",
    ]
    airports_filename = f"gs://{bucket}/flights/airports/airports.csv.gz"
    flights_output = f"gs://{bucket}/flights/tzcorr/all_flights"

    with beam.Pipeline(argv=argv) as pipeline:
        airports = (
            pipeline
            | "airports:read" >> beam.io.ReadFromText(airports_filename)
            | beam.Filter(lambda line: "United States" in line)
            | "airports:fields" >> beam.Map(lambda line: next(csv.reader([line])))
            | "airports:tz"
            >> beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
        )

        flights = (
            pipeline
            | "flights:read"
            >> beam.io.ReadFromBigQuery(
                query="SELECT * FROM dsongcp.flights WHERE rand() < 0.001",
                use_standard_sql=True,
            )
            | "flights:tzcorr" >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
        )

        (
            flights
            | "flights:tostring" >> beam.Map(lambda fields: json.dumps(fields))
            | "flights:gcsout" >> beam.io.textio.WriteToText(flights_output)
        )

        flights_schema = ",".join(
            [
                "FL_DATE:date",
                "UNIQUE_CARRIER:string",
                "ORIGIN_AIRPORT_SEQ_ID:string",
                "ORIGIN:string",
                "DEST_AIRPORT_SEQ_ID:string",
                "DEST:string",
                "CRS_DEP_TIME:timestamp",
                "DEP_TIME:timestamp",
                "DEP_DELAY:float",
                "TAXI_OUT:float",
                "WHEELS_OFF:timestamp",
                "WHEELS_ON:timestamp",
                "TAXI_IN:float",
                "CRS_ARR_TIME:timestamp",
                "ARR_TIME:timestamp",
                "ARR_DELAY:float",
                "CANCELLED:boolean",
                "DIVERTED:boolean",
                "DISTANCE:float",
                "DEP_AIRPORT_LAT:float",
                "DEP_AIRPORT_LON:float",
                "DEP_AIRPORT_TZOFFSET:float",
                "ARR_AIRPORT_LAT:float",
                "ARR_AIRPORT_LON:float",
                "ARR_AIRPORT_TZOFFSET:float",
                "Year:string",
            ]
        )

        # autodetect on JSON works, but is less reliable
        # flights_schema = 'SCHEMA_AUTODETECT'

        (
            flights
            | "flights:bqout"
            >> beam.io.WriteToBigQuery(
                "dsongcp.flights_tzcorr",
                schema=flights_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        events = flights | beam.FlatMap(get_next_event)
        events_schema = ",".join(
            [flights_schema, "EVENT_TYPE:string,EVENT_TIME:timestamp,EVENT_DATA:string"]
        )

        (
            events
            | "events:totablerow" >> beam.Map(lambda fields: create_event_row(fields))
            | "events:bqout"
            >> beam.io.WriteToBigQuery(
                "dsongcp.flights_simevents",
                schema=events_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run pipeline on the cloud")
    parser.add_argument("-p", "--project",
                        help="Unique project ID", required=True)
    parser.add_argument(
        "-b",
        "--bucket",
        help="Bucket where gs://BUCKET/flights/airports/airports.csv.gz exists",
        required=True,
    )

    args = vars(parser.parse_args())

    print("Correcting timestamps and writing to BigQuery dataset")

    run(project=args["project"], bucket=args["bucket"])
