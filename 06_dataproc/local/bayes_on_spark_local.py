#!/usr/bin/env python3

# pyright: reportCallIssue=false
# pyright: reportAttributeAccessIssue =false

""" Modelo de Bayes en Spark"""

import logging
import argparse
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def run_bayes(bucket: str):
    """Ejecuta el modelo en pyspark"""
    # Inicia sesión en Spark
    spark = SparkSession.builder.appName(
        "Bayes classification using Spark"
    ).getOrCreate()

    # Lee los archivos que comienzan con all_flights-*
    inputs = f"gs://{bucket}/flights/tzcorr/all_flights-*"
    flights = spark.read.json(inputs)
    flights.createOrReplaceTempView("flights")

    # Lee de la carpeta flights del bucket, el archivo trainday.csv (which days are training days?)
    train_days = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(f"gs://{bucket}/flights/trainday.csv")
    )
    # Crea una vista de train_days (El que trae el tag de cual fila es de entrenamiento o no)
    train_days.createOrReplaceTempView("traindays")

    # Crea el conjunto de datos de entrenamiento flights (create training dataset)
    statement = """
    SELECT
        f.FL_DATE AS date,
        CAST(distance AS FLOAT) AS distance,
        dep_delay,
        IF(arr_delay < 15, 1, 0) AS ontime
    FROM
        flights f
    JOIN
        traindays t
    ON
        f.FL_DATE == t.FL_DATE
    WHERE
        t.is_train_day AND
        f.dep_delay IS NOT NULL
    ORDER BY
        f.dep_delay DESC
    """
    flights = spark.sql(statement)

    # Cuantiles
    dist_thresh = flights.approxQuantile(
        "distance", list(np.arange(0, 1.0, 0.2)), 0.02
    )
    dist_thresh[-1] = float("inf")
    delay_thresh = range(10, 20)
    logging.info("Computed distance thresholds: %s", dist_thresh)

    # Bayes en cada categoría (bin)
    df = pd.DataFrame(columns=["dist_thresh", "delay_thresh", "frac_ontime"])
    for m in range(0, len(dist_thresh) - 1):
        for n in range(0, len(delay_thresh) - 1):
            bdf = flights[
                (flights["distance"] >= dist_thresh[m])
                & (flights["distance"] < dist_thresh[m + 1])
                & (flights["dep_delay"] >= delay_thresh[n])
                & (flights["dep_delay"] < delay_thresh[n + 1])
            ]
            on_time_frac = (
                bdf.agg(F.sum("ontime")).collect()[0][0]
                / bdf.agg(F.count("ontime")).collect()[0][0]
            )
            print(m, n, on_time_frac)
            df = df.append(
                {
                    "dist_thresh": dist_thresh[m],
                    "delay_thresh": delay_thresh[n],
                    "frac_ontime": on_time_frac,
                },
                ignore_index=True,
            )

    # lookup table
    df["score"] = abs(df["frac_ontime"] - 0.7)
    bayes = (
        df.sort_values(["score"])
        .groupby("dist_thresh")
        .head(1)
        .sort_values("dist_thresh")
    )
    bayes.to_csv(f"gs://{bucket}/flights/bayes.csv", index=False)
    logging.info("Wrote lookup table: %s", bayes.head())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create Bayes lookup table")
    parser.add_argument(
        "--bucket", help="GCS bucket to read/write data", required=True
    )
    parser.add_argument(
        "--debug",
        dest="debug",
        action="store_true",
        help="Specify if you want debug messages",
    )
    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(
            format="%(levelname)s: %(message)s", level=logging.DEBUG
        )
    else:
        logging.basicConfig(
            format="%(levelname)s: %(message)s", level=logging.INFO
        )

    run_bayes(args.bucket)
