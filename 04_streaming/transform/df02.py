#!/usr/bin/env python3


"""Pipeline para aeropuertos con zona horaria"""

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

import csv
import timezonefinder
from pytz.exceptions import UnknownTimeZoneError
import apache_beam as beam

# pyright: reportPrivateImportUsage=false
# pyright: reportAttributeAccessIssue=false


def addtimezone(lat, lon):
    """
    Agrega la zona horaria correspondiente a las coordenadas proporcionadas.
    """

    try:
        # Creamos una instancia de la clase para que sea re-usada
        tf = timezonefinder.TimezoneFinder()
        # Comprobar en qué zona horaria se encuentra un punto
        tz = tf.timezone_at(lng=float(lon), lat=float(lat))
        if tz is None:
            tz = 'UTC'
        return lat, lon, tz
    except (ValueError, UnknownTimeZoneError):
        return lat, lon, 'TIMEZONE'  # header


if __name__ == '__main__':
    with beam.Pipeline('DirectRunner') as pipeline:
        airports = (pipeline
                    | beam.io.ReadFromText('airports_2024.csv.gz')
                    | beam.Filter(lambda line: "United States" in line)
                    | beam.Map(lambda line: next(csv.reader([line])))
                    | beam.Map(
                        lambda fields: (
                            fields[0], addtimezone(fields[21], fields[26])
                        )
                    )
                    )

        airports_with_tz = (airports
                            | beam.Map(lambda f: f"{f[0]},{','.join(f[1])}")
                            | beam.io.textio.WriteToText(
                                'df02_airports_with_tz'
                            )
                            )
