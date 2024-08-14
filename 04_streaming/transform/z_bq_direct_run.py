#!/usr/bin/env python3

#  Copyright 2022 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=unused-variable
# pyright: reportPrivateImportUsage=false


# standard libraries
import logging

# third party libraries
import apache_beam as beam
from apache_beam import Map
from apache_beam.io import ReadFromBigQuery
from apache_beam.options.pipeline_options import PipelineOptions


class ReadQueryOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add a command line flag to be parsed along with other
        # normal PipelineOptions. This flag will store the SQL query
        # to be run against BigQuery.
        parser.add_argument(
            "--query",
            default=(
                "SELECT repository_language, COUNT(repository_language) AS totalRepos "  # noqa:E501
                "FROM `bigquery-public-data.samples.github_timeline` "  # noqa:E501
                "GROUP BY 1 "  # noqa:E501
                #"ORDER BY totalRepos DESC "  # noqa:E501
                #"LIMIT 5"  # noqa:E501
            ),
            help="BigQuery query to read data",
        )


def run():
    options = ReadQueryOptions(
        project="bigquery-manu-407202"
    )
    # Create a Beam pipeline with 2 steps:
    # run a query against BigQuery and log the results
    with beam.Pipeline(options=options) as p:
        output = (
            p
            | "ReadFromQuery"
            >> ReadFromBigQuery(
                method='DIRECT_READ',
                # method='EXPORT', Requiere un bucket para exportar
                query=options.query,
                use_standard_sql=True)
            | "LogData" >> Map(logging.info)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
