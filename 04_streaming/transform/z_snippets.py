#!/usr/bin/env python3


# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=unused-variable
# pyright: reportPrivateImportUsage=false
# pyright: reportUnusedExpression=false
# pyright: reportArgumentType=false

##############################################################################
################# bq_direct_run 1 ############################################
##############################################################################

# # standard libraries
# import argparse
# import logging

# # third party libraries
# import apache_beam as beam
# from apache_beam import Map
# from apache_beam.io import ReadFromBigQuery
# from apache_beam.options.pipeline_options import PipelineOptions


# class ReadQueryOptions(PipelineOptions):
#     @classmethod
#     def _add_argparse_args(cls, parser):
#         # Add a command line flag to be parsed along with other
#         # normal PipelineOptions. This flag will store the SQL query
#         # to be run against BigQuery.
#         parser.add_argument(
#             "--query",
#             default=(
#                 "SELECT repository_language, COUNT(repository_language) AS totalRepos "  # noqa:E501
#                 "FROM `bigquery-public-data.samples.github_timeline` "  # noqa:E501
#                 "GROUP BY 1 "  # noqa:E501
#                 #"ORDER BY totalRepos DESC "  # noqa:E501
#                 #"LIMIT 5"  # noqa:E501
#             ),
#             help="BigQuery query to read data",
#         )


# def run():
#     options = ReadQueryOptions(
#         project="bigquery-manu-407202"
#     )
#     # Create a Beam pipeline with 2 steps:
#     # run a query against BigQuery and log the results
#     with beam.Pipeline(options=options) as p:
#         output = (
#             p
#             | "ReadFromQuery"
#             >> ReadFromBigQuery(
#                 method='DIRECT_READ',
#                 # method='EXPORT', Requiere un bucket para exportar
#                 query=options.query,
#                 use_standard_sql=True)
#             | "LogData" >> Map(logging.info)
#         )


# if __name__ == "__main__":
#     logging.getLogger().setLevel(logging.INFO)
#     run()

##############################################################################
####################### Tornadoes ############################################
##############################################################################


# """A workflow using BigQuery sources and sinks.

# The workflow will read from a table that has the 'month' and 'tornado' fields as
# part of the table schema (other additional fields are ignored). The 'month'
# field is a number represented as a string (e.g., '23') and the 'tornado' field
# is a boolean field.

# The workflow will compute the number of tornadoes in each month and output
# the results to a table (created if needed) with the following schema:

# - month: number
# - tornado_count: number

# This example uses the default behavior for BigQuery source and sinks that
# represents table rows as plain Python dictionaries.
# """

# # pytype: skip-file

# import argparse
# import logging

# import apache_beam as beam


# def count_tornadoes(input_data):
#     """Workflow computing the number of tornadoes for each month that had one.

#     Args:
#       input_data: a PCollection of dictionaries representing table rows. Each
#         dictionary will have a 'month' and a 'tornado' key as described in the
#         module comment.

#     Returns:
#       A PCollection of dictionaries containing 'month' and 'tornado_count' keys.
#       Months without tornadoes are skipped.
#     """

#     return (
#         input_data
#         | 'months with tornadoes' >> beam.FlatMap(
#             lambda row: [(int(row['month']), 1)] if row['tornado'] else [])
#         | 'monthly count' >> beam.CombinePerKey(sum)
#         | 'format' >>
#         beam.Map(lambda k_v: {
#             'month': k_v[0], 'tornado_count': k_v[1]
#         }))


# def run(argv=None):
#     parser = argparse.ArgumentParser()
#     parser.add_argument(
#         '--input',
#         default='apache-beam-testing.samples.weather_stations',
#         help=(
#             'Input BigQuery table to process specified as: '
#             'PROJECT:DATASET.TABLE or DATASET.TABLE.'))
#     parser.add_argument(
#         '--output',
#         required=True,
#         help=(
#             'Output BigQuery table for results specified as: '
#             'PROJECT:DATASET.TABLE or DATASET.TABLE.'))

#     parser.add_argument(
#         '--gcs_location',
#         required=False,
#         help=('GCS Location to store files to load '
#               'data into Bigquery'))

#     known_args, pipeline_args = parser.parse_known_args(argv)

#     with beam.Pipeline(argv=pipeline_args) as p:

#         # Read the table rows into a PCollection.
#         rows = p | 'read' >> beam.io.ReadFromBigQuery(table=known_args.input)
#         counts = count_tornadoes(rows)

#         # Write the output using a "Write" transform that has side effects.
#         # pylint: disable=expression-not-assigned
#         counts | 'Write' >> beam.io.WriteToBigQuery(
#             known_args.output,
#             schema='month:INTEGER, tornado_count:INTEGER',
#             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
#             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

#         # Run the pipeline (all operations are deferred until run() is called).


# if __name__ == '__main__':
#     logging.getLogger().setLevel(logging.INFO)
#     run()


##############################################################################
################################## BQ Table ##################################
##############################################################################

# import argparse
import logging

import apache_beam as beam


# Quiet some pylint warnings that happen because of the somewhat special
# format for the code snippets.
# pylint:disable=invalid-name
# pylint:disable=expression-not-assigned
# pylint:disable=redefined-outer-name
# pylint:disable=unused-variable
# pylint:disable=consider-using-f-string


def model_bigqueryio_xlang(
        pipeline, write_project='', write_dataset='', write_table=''):
    """Examples for cross-language BigQuery sources and sinks."""

    # to avoid a validation error(input data schema and the table schema)
    # use a table that does not exist
    table_spec = '{}:{}.{}'.format(write_project, write_dataset, write_table)

    # [START model_bigqueryio_write_schema]
    table_schema = {
        'fields': [{
            'name': 'source', 'type': 'STRING', 'mode': 'NULLABLE'
        }, {
            'name': 'quote', 'type': 'STRING', 'mode': 'REQUIRED'
        }]
    }
    # [END model_bigqueryio_write_schema]

    quotes = pipeline | beam.Create([
        {
            'source': 'Mahatma Gandhi', 'quote': 'My life is my message.'
        },
        {
            'source': 'Yoda', 'quote': "Do, or do not. There is no 'try'."
        },
    ])

    quotes | "WriteTableWithStorageAPI" >> beam.io.WriteToBigQuery(
        table=table_spec,
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        # method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API
    )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    projectId = "bigquery-manu-407202"
    regionID = "southamerica-west1"
    datasetId = "dsongcp"
    tableId = "source_quote"
    with beam.Pipeline(runner="DirectRunner") as pipeline:
        model_bigqueryio_xlang(pipeline, projectId, datasetId, tableId)
