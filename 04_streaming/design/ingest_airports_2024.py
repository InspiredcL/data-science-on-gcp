#!/usr/bin/env python3

""" 
Descarga archivo airports desde el sitio de la BTS.

Después de descargar procesa el archivo para obtener un formato adecuado
con respecto al archivo original del bucket.
"""
from datetime import datetime
import gzip
import csv
import os
import zipfile
import logging
import requests
import bs4

# pyright: reportCallIssue=false
# pyright: reportIndexIssue=false
# pyright: reportAttributeAccessIssue=false


def procesar_respuesta_get(response: requests.Response) -> dict:
    """
    Procesa la respuesta de la solicitud GET y retorna los datos (payload)

    Args:
        `response` (requests.Response): Respuesta de la solicitud GET.
    Returns:
        `dict`: Diccionario con los datos extraídos de la respuesta.
    Raises:
        `ValueError`: Si no se encuentra el formulario con nombre `form1`.
    """

    # Analizar la respuesta
    soup = bs4.BeautifulSoup(response.text, 'html.parser')
    # Encontrar el formulario con nombre "form1"
    form1 = soup.find(
        name='form',
        attrs={'id': 'form1'}
    )
    # Verificar si se encontró el formulario antes de intentar acceder a los elementos
    if not form1:
        raise ValueError(
            "¡Error! No se encontró el formulario con nombre 'form1'.")
    # Modificar las variables chkAllVars y btnDownload
    chkallvars_input = form1.find(
        name='input',
        attrs={'name': 'chkAllVars'},
    )
    if chkallvars_input:
        chkallvars_input['value'] = 'on'
    btndownload_input = form1.find(
        name='input',
        attrs={'name': 'btnDownload'},
    )
    if btndownload_input:
        btndownload_input['value'] = 'Download'
    # Construir la carga útil para la solicitud POST
    payload = {
        input_tag['name']: input_tag.get('value', '')
        for input_tag in form1.find_all(name='input')
    }
    return payload


def download(destdir: str) -> str:
    """
    Descarga datos de la tabla `Aviation Support Tables : Master Coordinate`
    de la BTS

    Args:
        destdir (str): Directorio donde se guardará el archivo descargado.
    Returns:
        str: Ruta absoluta del archivo descargado.
    Raises:
        URLError: Si ocurre un error al descargar el archivo desde la URL.
    Notas:
        - El archivo descargado tendrá el nombre T_MASTER_CORD.zip
        - La función utiliza el módulo `logging` para registrar mensajes de
          información y depuración.
    """

    # URL de la página para descargar los datos
    URL = "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FLL&QO_fu146_anzr=N8vn6v10+f722146+gnoyr5"
    # Crear la sesión
    session = requests.Session()
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Host': 'www.transtats.bts.gov',
        'Referer': 'https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FLL&QO_fu146_anzr=N8vn6v10+f722146+gnoyr5',
        'Origin': 'https://www.transtats.bts.gov'
    }
    # Actualizar los encabezados
    session.headers.update(headers)
    # Obtener la página web con la sesión, para seleccionar los campos
    # a descargar
    response = session.get(URL, verify=False)
    ##########################################################################
    # Procesar la respuesta y obtener la carga útil
    payload = procesar_respuesta_get(response)
    # Construir la ruta completa al archivo T_MASTER_CORD.zip en el directorio de destino
    zip_file_path = os.path.join(destdir, "T_MASTER_CORD.zip")
    # Enviar la solicitud POST con la sesión
    response = session.post(URL, data=payload, verify=False)
    # Guardar el archivo T_MASTER_CORD.zip
    with open(zip_file_path, "wb") as zip_file:
        zip_file.write(response.content)
    return os.path.abspath(zip_file_path)


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
        data (list): Lista de listas que representan las filas y columnas
        del archivo CSV.
        header (list): Lista que representa la primera fila (encabezado)
        del archivo CSV.
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
                        "Fecha en la columna %s formateada correctamente",
                        columna)
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


def escribir_csv_gz(data, header, directorio_destino):
    """Escribe una lista de datos a un archivo CSV comprimido con gzip y retorna el archivo.

    Args:
        data (list): Lista de listas que representan las filas y columnas
        del archivo CSV.
        header (list): Lista que representa la primera fila (encabezado)
        del archivo CSV.
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


def run():
    """Función principal que ejecuta el procesamiento del script."""

    # Directorio de destino para la descarga
    directorio_destino = "/home/inspired/data-science-on-gcp/04_streaming/transform"
    # directorio_destino = "/home/inspired/data-science-on-gcp/04_streaming/design"
    # Obtener la ruta del archivo descargado (T_MASTER_CORD.zip)
    archivo_descargado = download(destdir=directorio_destino)
    # Descomprimir el archivo (T_MASTER_CORD.zip)
    archivo_csv = descomprimir_archivo(archivo_descargado, directorio_destino)
    # Leer el archivo CSV (T_MASTER_CORD.csv)
    data, header = leer_csv(archivo_csv)
    # Transformar las fechas
    transformar_fechas(data, header)
    # [Opcional] Escribir a CSV sin comprimir
    escribir_csv(data, header, directorio_destino)
    # Escribir a CSV.gz
    escribir_csv_gz(data, header, directorio_destino)
    # Eliminar archivo T_MASTER_CORD.csv temporal
    # os.remove(archivo_csv)
    # Eliminar archivo T_MASTER_CORD.zip descargado sin procesar
    os.remove(archivo_descargado)


if __name__ == "__main__":
    run()
