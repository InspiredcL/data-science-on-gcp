#!/usr/bin/env python3

""" _summary_

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
from pytz.exceptions import UnknownTimeZoneError
import apache_beam as beam

# Definición de la función que agrega la zona horaria basada en coordenadas


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
    * La función maneja la excepción `UnknownTimeZoneError` en caso de que las
    coordenadas no sean válidas.
    """

    try:
        # Importar la librería timezonefinder, recomendado por DataFlow
        import timezonefinder  # pylint: disable=import-outside-toplevel
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
        # Verifica si hay una hora válida y una zona horaria proporcionada
        if len(hhmm) > 0 and tzone is not None:
            # Importa los módulos datetime y pytz para manejar fechas y zonas horarias
            import datetime
            import pytz  # pylint: disable=import-outside-toplevel, multiple-imports

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
                    | 'airports:tz' >> beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
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
