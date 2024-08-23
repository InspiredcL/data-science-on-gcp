#!/usr/bin/env python3

""" Procesa el archivo T_MASTER_CORD.zip """

import zipfile
import os
import logging
import csv
import gzip
from datetime import datetime

# Configurar el sistema de registro
logging.basicConfig(level=logging.INFO)

# Descomprime el archivo
with zipfile.ZipFile("T_MASTER_CORD.zip", "r") as zip_ref:
    zip_ref.extractall()
csvfile = os.path.join(os.getcwd(), zip_ref.namelist()[0])
logging.info("Archivo %s descomprimido", csvfile)

# Leemos el CSV con el módulo csv
data = []
try:
    with open(csvfile, "r", encoding="utf-8") as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)  # Leer la primera fila como encabezado
        for row in csv_reader:
            data.append(row)
except Exception as e:
    logging.error("Error al leer el archivo CSV: %s", str(e))


# Transformamos al formato de entrada, mediante la creación de una función
def transformar_fecha(fila, columna):
    """transformar_fecha _summary_

    _extended_summary_

    Arguments:
        row -- _description_
        columna -- _description_
    """

    date_format = "%m/%d/%Y %I:%M:%S %p"
    try:
        if fila[columna]:
            fila[columna] = datetime.strptime(
                fila[columna], date_format
            ).strftime("%Y-%m-%d")
            logging.info(
                "Fecha en la columna %s formateada correctamente",
                header[columna],
            )
        else:
            logging.warning(
                "La fecha en la columna %s está vacía", header[columna]
            )
    except (IndexError, ValueError) as eerr:
        logging.warning(
            "No se pudo formatear la fecha en la columna %s: %s",
            header[columna],
            str(eerr),
        )


# Aplicamos la función transformar_fecha
for row in data:
    transformar_fecha(row, header.index("AIRPORT_START_DATE"))
    transformar_fecha(row, header.index("AIRPORT_THRU_DATE"))

# Escribimos a csv sin comprimir para revisión rápida
CSV_OUTPUT_FILE = "airports_2024.csv"
try:
    with open(CSV_OUTPUT_FILE, "w", newline="") as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(header)
        csv_writer.writerows(data)
    logging.info("Archivo CSV creado con éxito: %s", CSV_OUTPUT_FILE)
except Exception as e:
    logging.error("Error al escribir el archivo CSV: %s", str(e))

# Escribimos a csv.gz sin la primera columna de índice
GZ_OUTPUT_FILE = "airports_2024.csv.gz"
try:
    with gzip.open(GZ_OUTPUT_FILE, "wt", newline="") as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(header)
        csv_writer.writerows(data)
    logging.info("Archivo CSV comprimido creado con éxito: %s", GZ_OUTPUT_FILE)
except Exception as e:
    logging.error("Error al escribir el archivo CSV comprimido: %s", str(e))

# Eliminar archivo csv temporal
os.remove(csvfile)
