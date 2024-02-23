#!/usr/bin/env python3


""" _summary_

    _extended_summary_
"""
import zipfile
import os
import logging
import pandas as pd


# directorio_destino =
with zipfile.ZipFile("T_MASTER_CORD.zip", 'r') as zip_ref:
    zip_ref.extractall()
csvfile = os.path.join(os.getcwd(), zip_ref.namelist()[0])
print("Ruta del archivo csv:",csvfile)
logging.info("Archivo %s descomprimido", csvfile)

# Leemos el CSV
df = pd.read_csv(csvfile)

# Transformamos al formato de entrada
df['AIRPORT_START_DATE'] = pd.to_datetime(
    df['AIRPORT_START_DATE'], format="%m/%d/%Y %I:%M:%S %p"
)
df['AIRPORT_THRU_DATE'] = pd.to_datetime(
    df['AIRPORT_THRU_DATE'], format="%m/%d/%Y %I:%M:%S %p"
)

# Usamos dt.strftime
df['AIRPORT_START_DATE'] = df['AIRPORT_START_DATE'].dt.strftime('%Y-%m-%d')
df['AIRPORT_THRU_DATE'] = df['AIRPORT_THRU_DATE'].dt.strftime('%Y-%m-%d')

# Escribimos a csv.gz sin la primera columna de índice
df.to_csv("airports_2024.csv.gz", index=False, compression="gzip")

# Eliminar archivo csv temporal
os.remove(csvfile)
