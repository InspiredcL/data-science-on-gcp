#!/usr/bin/env python3
"""
Modulo para automatizar la carga de vuelos
"""

# Copyright 2016-2021 Google Inc.
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


import shutil
import logging
import os
import os.path
import zipfile
import tempfile
import ssl
from urllib.request import urlopen as impl
from urllib.error import URLError
from google.cloud import bigquery

# SOURCE = "https://storage.googleapis.com/data-science-on-gcp/edition2/raw"
SOURCE = "https://transtats.bts.gov/PREZIP"


def urlopen(url):
    """
    Abre una URL con un contexto SSL personalizado que deshabilita la
    verificación de certificados.
    Esta función es similar a `urllib.request.urlopen()`, pero permite
    ignorar los errores de verificación de certificados. Úselo con
    precaución, ya que puede exponer la comunicación a ataques de intermediario.
    Args:
        url (str): La URL que se abrirá.
    Returns:
        _UrlOpenRet: Un objeto similar a un archivo que se puede usar para
        leer la respuesta.
    Raises:
        URLError: Si hay un error al abrir la URL.
    """

    ctx_no_secure = ssl.create_default_context()
    ctx_no_secure.set_ciphers('HIGH:!DH:!aNULL')
    ctx_no_secure.check_hostname = False
    ctx_no_secure.verify_mode = ssl.CERT_NONE
    try:
        return impl(url, context=ctx_no_secure)
    except URLError as e:
        raise URLError(f"Error al abrir la URL '{url}': {e}") from e


def download(year: str, month: str, destdir: str) -> str:
    '''
    Descarga datos de la tabla `on-time performance` de la BTS

    Args:
        year (str): Año a procesar, en formato YYYY. Ejemplo: '2023'
        month (str): Mes a procesar, en formato MM. Ejemplo: '05' para Mayo
        destdir (str): Directorio donde se guardará el archivo descargado.
    Returns:
        str: Nombre del archivo descargado.
    Raises:
        URLError: Si ocurre un error al descargar el archivo desde la URL.
    Notas:
        - El archivo descargado tendrá el formato YYYYMM.zip.
        - La función utiliza el módulo `logging` para registrar mensajes de
        información y depuración.
    '''
    try:
        logging.info("Solicitando datos para %s-%s-*", year, month)

        url = os.path.join(
            SOURCE,
            f"On_Time_Reporting_Carrier_On_Time_Performance_1987\
            present_{year}_{int(month)}.zip"
        )
        logging.debug("Intentando descargar %s", url)

        nombre_archivo = os.path.join(destdir, "%s%s.zip", year, month)
        with open(nombre_archivo, "wb") as fp:
            response = urlopen(url)
            fp.write(response.read())
        logging.debug("%s guardado", nombre_archivo)
        return nombre_archivo
    except URLError as e:
        raise URLError(f"Error al descargar la URL '{url}': {e}") from e


def zip_to_csv(archivo_zip, directorio_destino):
    """
    Extrae y comprime un archivo CSV desde un archivo ZIP.

    Extrae el archivo CSV del archivo ZIP especificado y lo guarda en el
    directorio de destino. Adicionalmente, comprime el archivo CSV en
    formato GZIP para una subida más rápida.

    Args:
        archivo_zip (str): Ruta al archivo ZIP que contiene el archivo CSV.
        directorio_destino (str): Directorio donde se extraerá y guardará
        el archivo CSV.

    Returns:
        str: Ruta del archivo CSV comprimido en formato GZIP.

    Raises:
        BadZipFile: Si ocurre un error al abrir o extraer el archivo ZIP.
        OSError: Si hay problemas al manipular los archivos o directorios.
    """

    try:
        zip_ref = zipfile.ZipFile(archivo_zip, 'r')
        cwd = os.getcwd()
        os.chdir(directorio_destino)
        zip_ref.extractall()
        os.chdir(cwd)
        csvfile = os.path.join(directorio_destino, zip_ref.namelist()[0])
        zip_ref.close()
        logging.info("Archivo %s descomprimido", csvfile)
        return csvfile

    except zipfile.BadZipFile as e:
        raise zipfile.BadZipFile(
            f"Error al abrir o extraer el archivo ZIP {archivo_zip}: {e}"
        ) from e
    except OSError as e:
        raise OSError(
            f"Error al manipular archivos o directorios: {e}"
        ) from e


def bqload(file_path, year, month):
    '''
    Carga el archivo CSV en el dispositivo local a BigQuery, reemplazando
    los datos existentes en esa partición.
    '''

    client = bigquery.Client()
    # Truncar la partición existente
    table_ref = client.dataset('dsongcp').table(
        f'flights_raw${year}{month}'
    )
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = 'CSV'
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.ignore_unknown_values = True
    job_config.time_partitioning = bigquery.TimePartitioning(
        'MONTH', 'FlightDate')
    job_config.skip_leading_rows = 1
    job_config.schema = [
        bigquery.SchemaField(col_and_type.split(
            ':')[0], col_and_type.split(':')[1])  # , mode='required')
        for col_and_type in """
        Year:STRING,
        Quarter:STRING,
        Month:STRING,
        DayofMonth:STRING,
        DayOfWeek:STRING,
        FlightDate:DATE,
        Reporting_Airline:STRING,
        DOT_ID_Reporting_Airline:STRING,
        IATA_CODE_Reporting_Airline:STRING,
        Tail_Number:STRING,
        Flight_Number_Reporting_Airline:STRING,
        OriginAirportID:STRING,
        OriginAirportSeqID:STRING,
        OriginCityMarketID:STRING,
        Origin:STRING,
        OriginCityName:STRING,
        OriginState:STRING,
        OriginStateFips:STRING,
        OriginStateName:STRING,
        OriginWac:STRING,
        DestAirportID:STRING,
        DestAirportSeqID:STRING,
        DestCityMarketID:STRING,
        Dest:STRING,
        DestCityName:STRING,
        DestState:STRING,
        DestStateFips:STRING,
        DestStateName:STRING,
        DestWac:STRING,
        CRSDepTime:STRING,
        DepTime:STRING,
        DepDelay:STRING,
        DepDelayMinutes:STRING,
        DepDel15:STRING,
        DepartureDelayGroups:STRING,
        DepTimeBlk:STRING,
        TaxiOut:STRING,
        WheelsOff:STRING,
        WheelsOn:STRING,
        TaxiIn:STRING,
        CRSArrTime:STRING,
        ArrTime:STRING,
        ArrDelay:STRING,
        ArrDelayMinutes:STRING,
        ArrDel15:STRING,
        ArrivalDelayGroups:STRING,
        ArrTimeBlk:STRING,
        Cancelled:STRING,
        CancellationCode:STRING,
        Diverted:STRING,
        CRSElapsedTime:STRING,
        ActualElapsedTime:STRING,
        AirTime:STRING,
        Flights:STRING,
        Distance:STRING,
        DistanceGroup:STRING,
        CarrierDelay:STRING,
        WeatherDelay:STRING,
        NASDelay:STRING,
        SecurityDelay:STRING,
        LateAircraftDelay:STRING,
        FirstDepTime:STRING,
        TotalAddGTime:STRING,
        LongestAddGTime:STRING,
        DivAirportLandings:STRING,
        DivReachedDest:STRING,
        DivActualElapsedTime:STRING,
        DivArrDelay:STRING,
        DivDistance:STRING,
        Div1Airport:STRING,
        Div1AirportID:STRING,
        Div1AirportSeqID:STRING,
        Div1WheelsOn:STRING,
        Div1TotalGTime:STRING,
        Div1LongestGTime:STRING,
        Div1WheelsOff:STRING,
        Div1TailNum:STRING,
        Div2Airport:STRING,
        Div2AirportID:STRING,
        Div2AirportSeqID:STRING,
        Div2WheelsOn:STRING,
        Div2TotalGTime:STRING,
        Div2LongestGTime:STRING,
        Div2WheelsOff:STRING,
        Div2TailNum:STRING,
        Div3Airport:STRING,
        Div3AirportID:STRING,
        Div3AirportSeqID:STRING,
        Div3WheelsOn:STRING,
        Div3TotalGTime:STRING,
        Div3LongestGTime:STRING,
        Div3WheelsOff:STRING,
        Div3TailNum:STRING,
        Div4Airport:STRING,
        Div4AirportID:STRING,
        Div4AirportSeqID:STRING,
        Div4WheelsOn:STRING,
        Div4TotalGTime:STRING,
        Div4LongestGTime:STRING,
        Div4WheelsOff:STRING,
        Div4TailNum:STRING,
        Div5Airport:STRING,
        Div5AirportID:STRING,
        Div5AirportSeqID:STRING,
        Div5WheelsOn:STRING,
        Div5TotalGTime:STRING,
        Div5LongestGTime:STRING,
        Div5WheelsOff:STRING,
        Div5TailNum:STRING
        """.split(
            ',')
    ]
    with open(file_path, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file, table_ref, job_config=job_config
        )

    load_job.result()  # Espera a que la tabla cargue por completo
    if load_job.state != 'DONE':
        # Verificar si funciona el comando load_job.exception()
        # raise ValueError(f"Error al cargar los datos: {load_job.state}")
        raise ValueError(f"Error al cargar los datos: {load_job.exception()}")
    return table_ref, load_job.output_rows


def ingest(year, month, file_path):
    '''
    ingest flights data from BTS website to Google Cloud Storage
    return table, numrows on success.
    raises exception if this data is not on BTS website
    '''
    tempdir = tempfile.mkdtemp(prefix='ingest_flights')
    try:
        zip_file = download(year, month, tempdir)
        file_path = zip_to_csv(zip_file, tempdir)
        return bqload(file_path, year, month)
    except URLError as e:
        logging.error("Error downloading data: %s", e)
        raise URLError(
            f"Datos para {year}-{month} no disponibles en BTS.") from e
    except OSError as e:
        logging.error("File operation error: %s", e)
        raise OSError(f"Error processing files for {year}-{month}.") from e
    except ValueError as e:
        logging.error("Data parsing error: %s", e)
        raise ValueError(f"Error parsing data for {year}-{month}.") from e
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        raise ValueError(f"Error loading data for {year}-{month}.") from e
    finally:
        logging.debug('Cleaning up by removing %s', tempdir)
        shutil.rmtree(tempdir)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='ingesta de datos de vuelos desde el sitio web de BTS a \
        Google Cloud Storage')
    parser.add_argument(
        '--path_file', help='Desde donde cargar los datos', required=True)
    parser.add_argument(
        '--year', help='Ejemplo: 2015.  Si no se indica, se obtiene por \
        defecto el mes siguiente.', required=True)
    parser.add_argument(
        '--month', help='Especifique 01 para enero. Si no se indica, se \
        obtiene por defecto el mes siguiente.', required=True)
    parser.add_argument(
        '--debug', dest='debug', action='store_true',
        help='Mensaje de debug')

    try:
        args = parser.parse_args()
        if args.debug:
            logging.basicConfig(
                format='%(levelname)s: %(message)s', level=logging.DEBUG)
        else:
            logging.basicConfig(
                format='%(levelname)s: %(message)s', level=logging.INFO)

        year_ = args.year
        month_ = args.month
        logging.debug('Procesando año=%s y mes=%s', year_, month_)
        tableref, numrows = ingest(year_, month_, args.path_file)
        logging.info(
            'Success ... ingested %s rows to %s', numrows, tableref
        )
    except Exception as er:
        logging.exception("Error al procesar datos para el año %s y mes %s.\
            Intente nuevamente más tarde o verifique los parámetros de entrada", year_, month_)
        raise ValueError from er
