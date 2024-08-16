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


""" Desarrollo - Creación de Eventos. """

import logging
import csv
import json
import datetime
import apache_beam as beam
from pytz.exceptions import UnknownTimeZoneError
import timezonefinder
import pytz


# pylint: disable=expression-not-assigned
# pylint: disable=unnecessary-lambda
# pyright: reportPrivateImportUsage=false
# pyright: reportUnusedExpression=false
# pyright: reportOptionalMemberAccess=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportGeneralTypeIssues =false

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'


def addtimezone(
    lat: str, lon: str
) -> tuple[float | str, float | str, str | None]:
    """Agrega la zona horaria correspondiente."""

    try:
        tf = timezonefinder.TimezoneFinder()
        lat_f = float(lat)
        lon_f = float(lon)
        return lat_f, lon_f, tf.timezone_at(lng=lat_f, lat=lon_f)
    except (ValueError, UnknownTimeZoneError):
        return lat, lon, 'TIMEZONE'  # header


def as_utc(date, hhmm, tzone):
    """Convierte una fecha y hora en formato UTC."""

    try:
        if len(hhmm) > 0 and tzone is not None:

            loc_tz = pytz.timezone(tzone)
            loc_dt = loc_tz.localize(
                datetime.datetime.strptime(date, '%Y-%m-%d'),
                is_dst=False
            )
            # Considera las horas 2400 y 0000
            loc_dt += datetime.timedelta(
                hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
            utc_dt = loc_dt.astimezone(pytz.utc)
            return (
                utc_dt.strftime(DATETIME_FORMAT),
                loc_dt.utcoffset().total_seconds()
            )
        # Vuelos cancelados y offset de 0
        return '', 0
    except ValueError as e:
        logging.exception("%s %s %s ValueError: %s", date, hhmm, tzone, e)
        print("Exception occurred in as_utc:", e)
        return None


def add_24h_if_before(arr_time, dep_time):
    """Agrega 24 horas a la hora de llegada."""

    if len(arr_time) > 0 and len(dep_time) > 0 and arr_time < dep_time:
        adt = datetime.datetime.strptime(arr_time, DATETIME_FORMAT)
        adt += datetime.timedelta(hours=24)
        return adt.strftime(DATETIME_FORMAT)
    else:
        return arr_time


def tz_correct(fields, airport_timezones):
    """Realiza un ajuste de zonas horarias."""

    try:
        # Convierte a UTC
        dep_airport_id = fields["ORIGIN_AIRPORT_SEQ_ID"]
        arr_airport_id = fields["DEST_AIRPORT_SEQ_ID"]
        dep_timezone = airport_timezones[dep_airport_id][2]
        arr_timezone = airport_timezones[arr_airport_id][2]
        for f in ["CRS_DEP_TIME", "DEP_TIME", "WHEELS_OFF"]:
            fields[f], deptz = as_utc(
                fields["FL_DATE"],
                fields[f],
                dep_timezone
            )
        for f in ["WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
            fields[f], arrtz = as_utc(
                fields["FL_DATE"],
                fields[f],
                arr_timezone
            )
        # Corrige Hora
        for f in ["WHEELS_OFF", "WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
            fields[f] = add_24h_if_before(fields[f], fields["DEP_TIME"])
        # Crea columnas
        fields["DEP_AIRPORT_LAT"] = airport_timezones[dep_airport_id][0]
        fields["DEP_AIRPORT_LON"] = airport_timezones[dep_airport_id][1]
        fields["DEP_AIRPORT_TZOFFSET"] = deptz
        fields["ARR_AIRPORT_LAT"] = airport_timezones[arr_airport_id][0]
        fields["ARR_AIRPORT_LON"] = airport_timezones[arr_airport_id][1]
        fields["ARR_AIRPORT_TZOFFSET"] = arrtz
        yield fields
    except KeyError as e:
        # En caso de que falte una clave en el diccionario.
        logging.exception(
            " Ignorando %s aeropuerto no conocido, KeyError Error: %s",
            fields, e
        )


def get_next_event(fields):
    """Determina el siguiente evento."""

    if len(fields["DEP_TIME"]) > 0:
        event = dict(fields)  # copia de linea json
        event["EVENT_TYPE"] = "departed"
        event["EVENT_TIME"] = fields["DEP_TIME"]
        for f in [
            "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "ARR_TIME",
                "ARR_DELAY", "DISTANCE"]:
            event.pop(f, None)  # No se conoce el dato a la hora de embarque
        yield event
    if len(fields["ARR_TIME"]) > 0:
        event = dict(fields)
        event["EVENT_TYPE"] = "arrived"
        event["EVENT_TIME"] = fields["ARR_TIME"]
        yield event


def run():
    """" Ejecuta para procesar y generar eventos simulados. """

    parte = "04"
    # Source
    folder = "/home/inspired/data-science-on-gcp/04_streaming/transform/files"
    airports_file = f"{folder}/airports_2024.csv.gz"
    # flights_file = f"{folder}/flights_sample_2024.json"
    flights_file = f"{folder}/data/flights/flights_000{parte}-of-00023.jsonl"
    # Sink
    flights_local_output = f"{folder}/data/tzcorr/all_flights_000{parte}-of-00023"
    events_local_output = f"{folder}/data/events/all_events_000{parte}-of-00023"

    with beam.Pipeline("DirectRunner") as pipeline:
        # Source 1
        airports = (
            pipeline
            | "airports:read" >> beam.io.ReadFromText(airports_file)
            | "airports:onlyUSA" >> beam.Filter(
                lambda line: "United States" in line
            )
            | "airports:fields" >> beam.Map(
                lambda line: next(csv.reader([line]))
            )
            | "airports:tz" >> beam.Map(
                lambda fields: (fields[0], addtimezone(fields[21], fields[26]))
            )
        )
        # Source 2
        flights = (
            pipeline
            | 'flights:read' >> beam.io.ReadFromText(flights_file)
            | 'flights:parse' >> beam.Map(lambda line: json.loads(line))
            | 'flights:tzcorr' >> beam.FlatMap(
                tz_correct,
                beam.pvalue.AsDict(airports)
            )
        )
        # Sink 1
        (
            flights
            | 'flights:tostring' >> beam.Map(lambda fields: json.dumps(fields))
            | 'flights:out' >> beam.io.textio.WriteToText(flights_local_output)
        )
        # Sink 2
        events = flights | beam.FlatMap(get_next_event)
        (
            events
            | 'events:tostring' >> beam.Map(lambda fields: json.dumps(fields))
            | 'events:out' >> beam.io.textio.WriteToText(events_local_output)
        )


if __name__ == '__main__':
    run()
