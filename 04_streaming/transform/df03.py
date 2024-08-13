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

    Raises:
        e: _description_

    Returns:
        _description_

    Yields:
        _description_
"""

import logging
import csv
import json
from pytz.exceptions import UnknownTimeZoneError
import apache_beam as beam

# pylint: disable=import-outside-toplevel
# pyright: reportPrivateImportUsage=false
# pyright: reportAttributeAccessIssue=false


def addtimezone(lat, lon):
    """
    Agrega la zona horaria correspondiente a las coordenadas proporcionadas.
    """

    try:
        # Importar la librería timezonefinder, recomendado por DataFlow
        import timezonefinder
        # Crear una instancia de TimezoneFinder
        tf = timezonefinder.TimezoneFinder()
        # Convertir las coordenadas a números de punto flotante
        lat = float(lat)
        lon = float(lon)
        # Devolver las coordenadas y la zona horaria correspondiente
        return lat, lon, tf.timezone_at(lng=lon, lat=lat)
    except (ValueError, UnknownTimeZoneError):
        # Manejo de excepción en caso de error de valor o timezone
        return lat, lon, 'TIMEZONE'  # Encabezado


def as_utc(date, hhmm, tzone):
    """ Convierte a UTC.

    Convierte una fecha y hora en formato UTC a la hora corregida para una
    zona horaria específica.
    """

    try:
        # Verifica si hay una hora válida y una zona horaria proporcionada
        if len(hhmm) > 0 and tzone is not None:
            # Para manejar fechas y zonas horarias
            import datetime
            import pytz
            # Crea un objeto de zona horaria utilizando la zona proporcionada
            loc_tz = pytz.timezone(tzone)
            # Convierte la fecha en un objeto datetime en la zona horaria local
            loc_dt = loc_tz.localize(
                datetime.datetime.strptime(date, '%Y-%m-%d'),
                is_dst=False
            )
            # La hora se divide en horas y minutos, y se agrega a la fecha y hora local
            loc_dt += datetime.timedelta(
                hours=int(hhmm[:2]),
                minutes=int(hhmm[2:])
            )
            # Convierte la fecha y hora local en UTC
            utc_dt = loc_dt.astimezone(pytz.utc)
            # Retorna la fecha y hora en formato de cadena 'YYYY-MM-DD HH:MM:SS'
            return utc_dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            # Si no hay hora válida o zona horaria, retorna una cadena vacía
            return ''  # Una cadena vacía corresponde a vuelos cancelados
    except ValueError as e:
        # Si ocurre un error de ValueError, registra la excepción y vuelve a lanzarla
        logging.exception("%s %s %s ValueError: %s", date, hhmm, tzone, e)


# Definición de la función tz_correct que realiza correcciones de zonas horarias.
def tz_correct(line, airport_timezones):
    """ Correcciones de zonas horarias."""

    # Cargamos los campos del registro JSON en un diccionario llamado "fields".
    fields = json.loads(line)
    try:
        # Obtenemos el ID del aeropuerto de origen y destino.
        dep_airport_id = fields["ORIGIN_AIRPORT_SEQ_ID"]
        arr_airport_id = fields["DEST_AIRPORT_SEQ_ID"]
        # Obtenemos las zonas horarias de los aeropuertos de origen y destino.
        dep_timezone = airport_timezones[dep_airport_id][2]
        arr_timezone = airport_timezones[arr_airport_id][2]
        # Iteramos sobre las hhmm de embarque y las convertimos a UTC.
        for f in ["CRS_DEP_TIME", "DEP_TIME", "WHEELS_OFF"]:
            fields[f] = as_utc(fields["FL_DATE"], fields[f], dep_timezone)
        # Iteramos sobre las hhmm de llegada y las convertimos a UTC.
        for f in ["WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
            fields[f] = as_utc(fields["FL_DATE"], fields[f], arr_timezone)
        # Generamos una cadena JSON con los campos actualizados y la devolvemos.
        yield json.dumps(fields)
    except KeyError:
        # En caso de que falte una clave en el diccionario, registramos una excepción.
        logging.exception(
            " Ignorando %s porque el aeropuerto no es conocido",
            line
        )


if __name__ == '__main__':
    # Crear un flujo de trabajo de Beam con el modo 'DirectRunner'
    with beam.Pipeline('DirectRunner') as pipeline:
        # Leer el archivo 'airports.csv.gz' y filtrar líneas con "United States"
        airports = (pipeline
                    | 'airports:read' >> beam.io.ReadFromText(
                        'airports_2024.csv.gz')
                    | beam.Filter(lambda line: "United States" in line)
                    # Mapear cada línea a los campos correspondientes
                    | 'airports:fields' >> beam.Map(
                        lambda line: next(
                            csv.reader([line])
                        )
                    )
                    # Mapear los campos para agregar la zona horaria
                    | 'airports:tz' >> beam.Map(
                        lambda fields: (
                            fields[0], addtimezone(fields[21], fields[26]))
                    )
                    )

        # Leer el archivo 'flights_sample.json' y realizar corrección de zona horaria
        flights = (pipeline
                   | 'flights:read' >> beam.io.ReadFromText(
                       'flights_sample_2024.json'
                   )
                   | 'flights:tzcorr' >> beam.FlatMap(
                       tz_correct, beam.pvalue.AsDict(airports)
                   )
                   )

        # Escribir los resultados en un archivo 'all_flights'
        all_flights = (flights
                       | beam.io.textio.WriteToText('df03_all_flights')
                       )
