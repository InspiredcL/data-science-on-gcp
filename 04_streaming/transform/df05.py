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


""" _summary_

_extended_summary_
"""

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

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'


def addtimezone(lat: str, lon: str) -> tuple[float,float,str | None]:
    """Agrega la zona horaria correspondiente a las coordenadas proporcionadas.

    * La función utiliza la librería `timezonefinder` para obtener
    la zona horaria correspondiente a las coordenadas proporcionadas.
    * La función maneja la excepción `ValueError` en caso de que las
    coordenadas no sean válidas.

    Args:
        `lat` (str): Latitud en grados decimales.
        `lon` (str): Longitud en grados decimales.

    Returns:
        `tuple`:
            Una tupla con las coordenadas y la zona horaria correspondiente.
            Por ejemplo:
            addtimezone(-33.45, -70.66)
            (-33.45, -70.66, 'America/Santiago')

    Raises:
        `ValueError`: Si las coordenadas no son válidas.
    """

    try:
        import timezonefinder  # pylint: disable=import-outside-toplevel
        tf = timezonefinder.TimezoneFinder()
        lat_f = float(lat)
        lon_f = float(lon)
        return lat_f, lon_f, tf.timezone_at(lng=lat_f, lat=lon_f)
    except ValueError:
        return lat_f, lon_f, 'TIMEZONE'  # header


# def fetch_smalltable_rows(
#     table_handle: smalltable.Table,
#     keys: Sequence[bytes | str],
#     require_all_keys: bool = False,
# ) -> Mapping[bytes, tuple[str, ...]]:
#     """Fetches rows from a Smalltable.

#     Retrieves rows pertaining to the given keys from the Table instance
#     represented by table_handle.  String keys will be UTF-8 encoded.

#     Args:
#       table_handle:
#         An open smalltable.Table instance.
#       keys:
#         A sequence of strings representing the key of each table row to
#         fetch.  String keys will be UTF-8 encoded.
#       require_all_keys:
#         If True only rows with values set for all keys will be returned.

#     Returns:
#       A dict mapping keys to the corresponding table row data
#       fetched. Each row is represented as a tuple of strings. For
#       example:

#       {b'Serak': ('Rigel VII', 'Preparer'),
#        b'Zim': ('Irk', 'Invader'),
#        b'Lrrr': ('Omicron Persei 8', 'Emperor')}

#       Returned keys are always bytes.  If a key from the keys argument is
#       missing from the dictionary, then that row was not found in the
#       table (and require_all_keys must have been False).

#     Raises:
#       IOError: An error occurred accessing the smalltable.
#     """
#     pass


def as_utc(date, hhmm, tzone):
    """
    Convierte una fecha y hora en formato UTC a la hora corregida para una zona horaria específica.

    Args:
        `date`: Fecha en formato `YYYY-MM-DD`.
        `hhmm`: Hora en formato `HH:MM`.
        `tzone`: Zona horaria en formato `TZ`.

    Returns:
        `utc_dt`: Objeto de fecha y hora en formato `YYYY-MM-DDTHH:MM:SS+00:00`.
        `tz_offset`: Desplazamiento de la zona horaria en segundos.

    Raises:
    `ValueError`: Si la fecha, la hora o la zona horaria no son válidas.
    """

    try:
        if len(hhmm) > 0 and tzone is not None:
            import datetime
            import pytz  # pylint: disable=import-outside-toplevel
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
        else:
            # Vuelos cancelados y offset de 0
            print("Devolviendo ('', 0) porque hhmm está vacío or tzone es None")
            return '', 0
    except ValueError as e:
        logging.exception("%s %s %s ValueError: %s", date, hhmm, tzone, e)
        print("Exception occurred in as_utc:", e)
        return None


def add_24h_if_before(arrtime, deptime):
    """
    Agrega 24 horas a la hora de llegada (arrtime) si es anterior a la
    hora de salida (deptime).

    **Args:**
    * `arrtime`: Hora de llegada en formato `YYYY-MM-DDTHH:MM:SS`.
    * `deptime`: Hora de salida en formato `YYYY-MM-DDTHH:MM:SS`.

    **Returns:**

    * `arrtime` si la hora de llegada es posterior o igual a la hora de salida.
    * `arrtime` más 24 horas si la hora de llegada es anterior a la hora
    de salida.

    **Raises:**

    * `ValueError`: Si las horas de llegada o salida no están en formato válido.
    """

    import datetime  # pylint: disable=import-outside-toplevel
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

    **Args:**
    * `fields`: Diccionario que contiene los datos de vuelo, incluyendo
    campos de fecha y hora.
    * `airport_timezones`: Diccionario que mapea los identificadores de
    aeropuerto a sus respectivas zonas horarias.
    **Returns:**
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

    try:
        # convert all times to UTC
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

        for f in ["WHEELS_OFF", "WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
            fields[f] = add_24h_if_before(fields[f], fields["DEP_TIME"])

        fields["DEP_AIRPORT_LAT"] = airport_timezones[dep_airport_id][0]
        fields["DEP_AIRPORT_LON"] = airport_timezones[dep_airport_id][1]
        fields["DEP_AIRPORT_TZOFFSET"] = deptz
        fields["ARR_AIRPORT_LAT"] = airport_timezones[arr_airport_id][0]
        fields["ARR_AIRPORT_LON"] = airport_timezones[arr_airport_id][1]
        fields["ARR_AIRPORT_TZOFFSET"] = arrtz
        yield fields
    except KeyError as e:
        # En caso de que falte una clave en el diccionario, registramos una excepción.
        logging.exception(
            " Ignorando %s aeropuerto no conocido, KeyError Error: %s",
            fields,
            e
        )


def get_next_event(fields):
    """
    Determina el siguiente evento a partir de los campos disponibles.

    **Args:**
    * `fields`: Diccionario que contiene los datos de vuelo, incluyendo campos de fecha y hora.
    **Returns:**
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
    """"
    Ejecuta un pipeline de Apache Beam para procesar datos de vuelos y generar eventos simulados.
        * Configura los parámetros del pipeline de Beam.
        * Lee los datos de los aeropuertos desde un archivo CSV filtrado.
        * Lee los datos de los vuelos desde BigQuery.
        * Corrige las zonas horarias de los vuelos utilizando los datos de los aeropuertos.
        * Escribe los vuelos corregidos en formato JSON a un archivo de texto en GCS.
        * Escribe los vuelos corregidos en BigQuery.
        * Genera eventos simulados a partir de los vuelos corregidos.
        * Escribe los eventos simulados en BigQuery.

    **:**

    * `project`: Nombre del proyecto de Google Cloud Platform.
    * `bucket`: Nombre del bucket de Google Cloud Storage para almacenar archivos intermedios.
    * `region`: Región de Google Cloud para ejecutar el pipeline.

    **Ejecución independiente:**
    python run.py --project=<project_id> --bucket=<bucket_name> --region=<region>
    """

    with beam.Pipeline('DirectRunner') as pipeline:
        airports = (pipeline
                    | 'airports:read' >> beam.io.ReadFromText(
                        'airports_2024.csv.gz'
                    )
                    | beam.Filter(lambda line: "United States" in line)
                    | 'airports:fields' >> beam.Map(
                        lambda line: next(csv.reader([line]))
                    )
                    | 'airports:tz' >> beam.Map(
                        lambda fields: (fields[0],
                                        addtimezone(fields[21], fields[26]))
                    )
                    )

        flights = (pipeline
                   | 'flights:read' >> beam.io.ReadFromText(
                       'flights_sample_2024.json'
                   )
                   | 'flights:parse' >> beam.Map(lambda line: json.loads(line))
                   | 'flights:tzcorr' >> beam.FlatMap(
                       tz_correct,
                       beam.pvalue.AsDict(airports)
                   )
                   )

        (flights
         | 'flights:tostring' >> beam.Map(
             lambda fields: json.dumps(fields)
         )
         | 'flights:out' >> beam.io.textio.WriteToText(
             'df_05_all_flights'
         )
         )

        events = flights | beam.FlatMap(get_next_event)

        (events
         | 'events:tostring' >> beam.Map(lambda fields: json.dumps(fields))
         | 'events:out' >> beam.io.textio.WriteToText('df05_all_events')
         )


if __name__ == '__main__':
    run()
