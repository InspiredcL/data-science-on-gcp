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


"""Pipeline que transforma vuelos."""

import logging
import csv
import json
import datetime
import pytz
from pytz.exceptions import UnknownTimeZoneError
import apache_beam as beam
import timezonefinder


# pylint: disable=expression-not-assigned
# pyright: reportOptionalMemberAccess=false
# pyright: reportPrivateImportUsage=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportUnusedExpression=false


def addtimezone(lat, lon):
    """Agrega la zona horaria correspondiente"""

    try:
        tf = timezonefinder.TimezoneFinder()
        lat = float(lat)
        lon = float(lon)
        return lat, lon, tf.timezone_at(lng=lon, lat=lat)
    except (ValueError, UnknownTimeZoneError):
        return lat, lon, "TIMEZONE"  # header


def as_utc(date, hhmm, tzone):
    """Convierte una fecha y hora en formato UTC"""

    try:
        if len(hhmm) > 0 and tzone is not None:
            loc_tz = pytz.timezone(tzone)
            loc_dt = loc_tz.localize(
                datetime.datetime.strptime(date, "%Y-%m-%d"), is_dst=False
            )
            # Considera 2400 y 0000
            loc_dt += datetime.timedelta(
                hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
            utc_dt = loc_dt.astimezone(pytz.utc)
            return (
                utc_dt.strftime("%Y-%m-%d %H:%M:%S"),
                loc_dt.utcoffset().total_seconds(),
            )
        # Vuelos cancelados y offset de 0
        return "", 0
    except ValueError as e:
        logging.exception("%s %s %s ValueError: %s", date, hhmm, tzone, e)
        print("Exception occurred in as_utc:", e)
        return None


def add_24h_if_before(arrtime, deptime):
    """add_24h_if_before"""

    if len(arrtime) > 0 and len(deptime) > 0 and arrtime < deptime:
        adt = datetime.datetime.strptime(arrtime, "%Y-%m-%d %H:%M:%S")
        adt += datetime.timedelta(hours=24)
        return adt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        return arrtime


def tz_correct(line, airport_timezones):
    """Realiza un ajuste de zonas horarias"""

    fields = json.loads(line)
    try:
        # convert all times to UTC
        dep_airport_id = fields["ORIGIN_AIRPORT_SEQ_ID"]
        arr_airport_id = fields["DEST_AIRPORT_SEQ_ID"]
        dep_timezone = airport_timezones[dep_airport_id][2]
        arr_timezone = airport_timezones[arr_airport_id][2]

        for f in ["CRS_DEP_TIME", "DEP_TIME", "WHEELS_OFF"]:
            fields[f], deptz = as_utc(
                fields["FL_DATE"], fields[f], dep_timezone
            )  # type: ignore

        for f in ["WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
            fields[f], arrtz = as_utc(
                fields["FL_DATE"], fields[f], arr_timezone
            )  # type: ignore

        for f in ["WHEELS_OFF", "WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
            fields[f] = add_24h_if_before(fields[f], fields["DEP_TIME"])

        fields["DEP_AIRPORT_LAT"] = airport_timezones[dep_airport_id][0]
        fields["DEP_AIRPORT_LON"] = airport_timezones[dep_airport_id][1]
        fields["DEP_AIRPORT_TZOFFSET"] = deptz
        fields["ARR_AIRPORT_LAT"] = airport_timezones[arr_airport_id][0]
        fields["ARR_AIRPORT_LON"] = airport_timezones[arr_airport_id][1]
        fields["ARR_AIRPORT_TZOFFSET"] = arrtz
        yield json.dumps(fields)
        # yield fields
    except KeyError as e:
        # En caso de que falte una clave en el diccionario.
        logging.exception(
            " Ignorando %s aeropuerto no conocido, KeyError Error: %s",
            fields, e
        )


def run():
    """Ejecuta el pipeline."""
    # Source
    start=7
    end=8
    airports_file = "airports_2024.csv.gz"
    flights_files = [
        f"flights/chunks/flights_000{parte:02d}-of-00023.jsonl" for parte in range(start,end+1)
    ]
    # Sink
    flights_local_output = "flights/tzcorr/all_flights_2_to_10-of-23"

    with beam.Pipeline("DirectRunner") as pipeline:
        # Source 1
        airports = (
            pipeline
            | "airports:read" >> beam.io.ReadFromText(airports_file)
            | "airports:onlyUSA" >> beam.Filter(lambda line: "United States" in line)
            | "airports:fields" >> beam.Map(lambda line: next(csv.reader([line])))
            | "airports:tz"
            >> beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
        )
        # Source 2
        flights = []
        for i, flights_file in enumerate(flights_files):
            flights.append(
                pipeline
                | f"flights:read-{i}" >> beam.io.ReadFromText(flights_file)
                | f"flights:tzcorr-{i}"
                >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
            )
        # Sink
        (
            flights
            | beam.Flatten()
            # | beam.Map(lambda fields: json.dumps(fields))
            | beam.io.textio.WriteToText(flights_local_output)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    print(
        "Corrigiendo marcas de tiempo y escribiendo a un archivo local"
        "los vuelos"
    )
    run()