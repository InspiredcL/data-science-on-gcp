#!/usr/bin/env python3

"""Pipeline que transforma vuelos (Json) y guarda al dispositivo local"""

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


def as_utc(date, hh_mm, t_zone):
    """Convierte una fecha y hora en formato UTC"""

    try:
        if len(hh_mm) > 0 and t_zone is not None:
            loc_tz = pytz.timezone(t_zone)
            loc_dt = loc_tz.localize(
                datetime.datetime.strptime(date, "%Y-%m-%d"), is_dst=False
            )
            # Considera 2400 y 0000
            loc_dt += datetime.timedelta(
                hours=int(hh_mm[:2]), minutes=int(hh_mm[2:])
            )
            utc_dt = loc_dt.astimezone(pytz.utc)
            return (
                utc_dt.strftime("%Y-%m-%d %H:%M:%S"),
                loc_dt.utcoffset().total_seconds(),
            )
        # Vuelos cancelados y offset de 0
        return "", 0
    except (ValueError, UnknownTimeZoneError) as e:
        logging.exception("%s %s %s ValueError: %s", date, hh_mm, t_zone, e)
        print("Exception occurred in as_utc:", e)
        return "", 0


def add_24h_if_before(arr_time, dep_time):
    """add_24h_if_before"""

    if len(arr_time) > 0 and len(dep_time) > 0 and arr_time < dep_time:
        adt = datetime.datetime.strptime(arr_time, "%Y-%m-%d %H:%M:%S")
        adt += datetime.timedelta(hours=24)
        return adt.strftime("%Y-%m-%d %H:%M:%S")
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
                fields["FL_DATE"], fields[f], dep_timezone
            )
        for f in ["WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
            fields[f], arrtz = as_utc(
                fields["FL_DATE"], fields[f], arr_timezone
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
            fields,
            e,
        )


def get_next_event(fields):
    """Determina el siguiente evento de un vuelo"""

    # Salida
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
    # Aterrizaje
    if len(fields["WHEELS_OFF"]) > 0:
        event = dict(fields)  # copy
        event["EVENT_TYPE"] = "wheelsoff"
        event["EVENT_TIME"] = fields["WHEELS_OFF"]
        for f in ["WHEELS_ON", "TAXI_IN", "ARR_TIME", "ARR_DELAY", "DISTANCE"]:
            event.pop(f, None)  # not knowable at wheels off time
        yield event
    # Llegada
    if len(fields["ARR_TIME"]) > 0:
        event = dict(fields)
        event["EVENT_TYPE"] = "arrived"
        event["EVENT_TIME"] = fields["ARR_TIME"]
        yield event


def run_csv():
    """Ejecuta el pipeline."""

    # Source
    airports_file = "airports_test.csv"
    flights_file = "flights_test.json"
    # Sink
    flights_output = "local_all_flights"
    events_output = "local_all_events"
    with beam.Pipeline("DirectRunner") as pipeline:
        # Source 1
        airports = (
            pipeline
            | "airports:read" >> beam.io.ReadFromText(airports_file)
            | "airports:onlyUSA"
            >> beam.Filter(lambda line: "United States" in line)
            | "airports:fields"
            >> beam.Map(lambda line: next(csv.reader([line])))
            | "airports:tz"
            >> beam.Map(
                lambda fields: (
                    fields[0],
                    addtimezone(fields[21], fields[26]),
                )
            )
        )
        airports | beam.Map(print)
        # Source 2
        flights = (
            pipeline
            | "flights:read" >> beam.io.ReadFromText(flights_file)
            | "flights:parse" >> beam.Map(json.loads)
            | "flights:tzcorr"
            >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
        )
        # Sink 1
        (
            flights
            | "flights:tostring" >> beam.Map(json.dumps)
            | "flights:f_out" >> beam.io.textio.WriteToText(flights_output)
        )
        # Sink 2
        events = flights | beam.FlatMap(get_next_event)
        (
            events
            | "events:tostring" >> beam.Map(json.dumps)
            | "events:e_out" >> beam.io.textio.WriteToText(events_output)
        )


def run_json():
    """Ejecuta el pipeline."""

    # Source
    airports_file = "airports_test.json"
    flights_file = "flights_test.json"
    # Sink
    flights_output = "local_all_flights"
    events_output = "local_all_events"
    with beam.Pipeline("DirectRunner") as pipeline:
        # Source 1
        airports = (
            pipeline
            | "airports:read" >> beam.io.ReadFromText(airports_file)
            | "airports:fields" >> beam.Map(json.loads)
            # Compatibilidad con json
            | "airports:onlyUSA"
            >> beam.Filter(
                lambda field: field["AIRPORT_COUNTRY_NAME"] == "United States"
            )
            | "airports:tz"
            >> beam.Map(
                lambda fields: (
                    str(fields["AIRPORT_SEQ_ID"]),
                    addtimezone(fields["LATITUDE"], fields["LONGITUDE"]),
                )
            )
        )
        airports | beam.Map(print)  # Depuración
        # Source 2
        flights = (
            pipeline
            | "flights:read" >> beam.io.ReadFromText(flights_file)
            | "flights:parse" >> beam.Map(json.loads)
            | "flights:tzcorr"
            >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
        )
        # Sink 1
        (
            flights
            | "flights:tostring" >> beam.Map(json.dumps)
            | "flights:f_out" >> beam.io.textio.WriteToText(flights_output)
        )
        # Sink 2
        events = flights | beam.FlatMap(get_next_event)
        (
            events
            | "events:tostring" >> beam.Map(json.dumps)
            | "events:e_out" >> beam.io.textio.WriteToText(events_output)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    logging.info(
        " Corrigiendo marcas de tiempo y escribiendo a un archivo local"
        "los vuelos y los eventos\n"
    )
    # run_csv()
    run_json()
