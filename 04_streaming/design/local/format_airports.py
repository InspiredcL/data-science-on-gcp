#!/usr/bin/env python3

""" _summary_

_extended_summary_
"""
import zipfile
import os
import logging
import csv
import gzip
from datetime import datetime


# Descomprime el archivo
def descomprimir_archivo(nombre_archivo_zip, directorio_destino):
    """
    Descomprime un archivo ZIP en el directorio especificado.

    Extrae el archivo CSV del archivo ZIP especificado y lo guarda en el
    directorio de destino.

    Args:
        nombre_archivo_zip (str): Ruta al archivo ZIP que contiene el archivo CSV.
        directorio_destino (str): Directorio donde se extraerá el archivo CSV.

    Returns:
        str: Ruta del archivo CSV descomprimido.

    Raises:
        BadZipFile: Si ocurre un error al abrir o extraer el archivo ZIP.
        OSError: Si hay problemas al manipular los archivos o directorios.
    """

    try:
        archivo_zip = os.path.join(directorio_destino, nombre_archivo_zip)
        with zipfile.ZipFile(archivo_zip, 'r') as zip_ref:
            zip_ref.extractall(directorio_destino)
            archivo_csv = os.path.join(
                directorio_destino, zip_ref.namelist()[0])
            logging.info("Archivo %s descomprimido", archivo_csv)
            return archivo_csv

    except zipfile.BadZipFile as e:
        raise zipfile.BadZipFile(
            f"Error al abrir o extraer el archivo ZIP {archivo_zip}: {e}"
        ) from e
    except OSError as e:
        raise OSError(
            f"Error al manipular archivos o directorios: {e}"
        ) from e


# Leemos el CSV con el módulo csv a dos listas una con los encabezados
# y otra con los datos
def leer_csv(archivo_csv):
    """Lee un archivo CSV y devuelve los datos como una lista de listas y el encabezado.

    Args:
        archivo_csv (str): Ruta al archivo CSV.

    Returns:
        tuple: Un tuple que contiene dos elementos:
        - data (list): Lista de listas que representan las filas y columnas del archivo CSV.
        - header (list): Lista que representa la primera fila (encabezado) del archivo CSV.
    """

    data = []
    try:
        with open(archivo_csv, 'r', encoding="utf-8") as file:
            csv_reader = csv.reader(file)
            header = next(csv_reader)  # Leer la primera fila como encabezado
            for row in csv_reader:
                data.append(row)
    except csv.Error as e:
        logging.error("Error al leer el archivo CSV: %s", str(e))
        data = []
        header = []  # Set data and header to empty lists in case of error
    return data, header


# Transformamos al formato de entrada, mediante la creación de una función
def transformar_fechas(data, header):
    """Transforma las fechas en las columnas especificadas de un conjunto de datos.

    Args:
        data (list): Lista de listas que representan las filas y columnas del archivo CSV.
        header (list): Lista que representa la primera fila (encabezado) del archivo CSV.

    Returns:
        None
    """

    # Por inspección previa conocemos el formato de origen
    date_format = "%m/%d/%Y %I:%M:%S %p"
    # Conocemos cuales son las columnas con fecha
    columnas_fecha = ['AIRPORT_START_DATE', 'AIRPORT_THRU_DATE']

    for row in data:
        for columna in columnas_fecha:
            indice_columna = header.index(columna)
            try:
                if row[indice_columna]:
                    row[indice_columna] = datetime.strptime(
                        row[indice_columna], date_format).strftime('%Y-%m-%d')
                    logging.info(
                        "Fecha en la columna %s formateada correctamente", columna)
                else:
                    logging.warning(
                        "La fecha en la columna %s está vacía", columna)
            except (IndexError, ValueError) as e:
                logging.warning(
                    "No se pudo formatear la fecha en la columna %s: %s", columna, str(e))


# Escribimos a csv sin comprimir para revisión rápida
def escribir_csv(data, header, directorio_destino):
    """Escribe una lista de datos a un archivo CSV y retorna el archivo.

    Args:
        data (list): Lista de listas que representan las filas y columnas del archivo CSV.
        header (list): Lista que representa la primera fila (encabezado) del archivo CSV.
        output_file (str): Ruta al archivo CSV de salida.

    Returns:
        str: Ruta al archivo CSV creado.
    """

    archivo_csv = "airports_2024.csv"
    try:
        csv_output_file = os.path.join(directorio_destino, archivo_csv)
        with open(csv_output_file, 'w', newline='', encoding='utf-8') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(header)
            csv_writer.writerows(data)
        logging.info("Archivo CSV creado con éxito: %s", csv_output_file)
        return csv_output_file
    except csv.Error as e_csv:
        logging.error("Error al escribir el archivo CSV: %s", str(e_csv))
        return None

# Escribimos a csv.gz


def escribir_csv_gz(data, header, directorio_destino):
    """Escribe una lista de datos a un archivo CSV comprimido con gzip y retorna el archivo.

    Args:
        data (list): Lista de listas que representan las filas y columnas del archivo CSV.
        header (list): Lista que representa la primera fila (encabezado) del archivo CSV.
        gz_output_file (str): Ruta al archivo CSV comprimido de salida.

    Returns:
        str: Ruta al archivo CSV comprimido creado, o None en caso de error.
    """

    archivo_gzip = "airports_2024.csv.gz"
    try:
        gz_output_file = os.path.join(directorio_destino, archivo_gzip)
        with gzip.open(gz_output_file, 'wt', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(header)
            csv_writer.writerows(data)
        # Return the output file path on success
        return gz_output_file
    except gzip.BadGzipFile as e_gzip:
        logging.error(
            "Error al escribir el archivo CSV comprimido: %s", str(e_gzip))
        return None


def main():
    """Función principal que ejecuta el procesamiento del script original."""

    # Directorio de destino para la descarga
    directorio_destino = "/home/inspired/data-science-on-gcp/04_streaming/design"
    # Descomprimir el archivo
    archivo_csv = descomprimir_archivo("T_MASTER_CORD.zip", directorio_destino)

    # Leer el archivo CSV
    data, header = leer_csv(archivo_csv)

    # Transformar las fechas
    transformar_fechas(data, header)

    # Escribir a CSV sin comprimir
    escribir_csv(data, header, directorio_destino)

    # Escribir a CSV.gz
    escribir_csv_gz(data, header, directorio_destino)

    # Eliminar archivo csv temporal
    os.remove(archivo_csv)


if __name__ == "__main__":
    main()
