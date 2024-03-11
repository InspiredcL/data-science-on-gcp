#!/usr/bin/env python3


""" _summary_

_extended_summary_
"""

# Copyright 2019 Google Inc.
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
import apache_beam as beam
from apache_beam.pipeline import Pipeline
from apache_beam.io import WriteToText, ReadFromText


if __name__ == '__main__':
    with Pipeline('DirectRunner') as pipeline:
        airports = (pipeline
                    | ReadFromText('airports_2024.csv.gz')
                    | beam.Map(lambda line: next(csv.reader([line])))
                    | beam.Map(lambda fields: (fields[0], (fields[21], fields[26])))
                    )

        transformed_airports = (airports
                                | beam.Map(lambda airport_data: '{},{}'.format(
                                    airport_data[0], ','.join(airport_data[1])))
                                | WriteToText('extracted_airports')
                                )
