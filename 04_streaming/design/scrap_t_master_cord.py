#!/usr/bin/env python3

""" 
    _summary_

    _extended_summary_
"""
import os
import zipfile
from typing import Union
import requests
from bs4 import BeautifulSoup, Tag, NavigableString


# Obtiene la ruta del directorio actual del script
script_directory = os.path.dirname(os.path.abspath(__file__))

# Seleccionar de la tabla con los parámetros dataset: Aviation Support Tables
# y la tabla: Master Coordinate
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

# Obtener la página web con la sesión
response = session.get(URL, verify=False)
soup = BeautifulSoup(response.text, 'html.parser')

# Encontrar el formulario con nombre "form1"
form1 = soup.find('form', {'id': 'form1'})

# Verificar si se encontró el formulario antes de intentar acceder a los elementos
if form1:
    chkAllVars_input = form1.find(
        name='input',
        attrs={'name': 'chkAllVars'},
    )
    btnDownload_input: Union[Tag, NavigableString, None] = form1.find(
        name='input',
        attrs={'name': 'btnDownload'}
        )  # Type: Union[Tag, NavigableString, None]

    # Verificar si se encontraron los elementos antes de intentar acceder a ['value']
    if chkAllVars_input:
        chkAllVars_input['value'] = 'on'

    if btnDownload_input:
        btnDownload_input['value'] = 'Download'
else:
    print("¡Error! No se encontró el formulario con nombre 'form1'.")

# Construir la carga útil para la solicitud POST
payload = {input_tag['name']: input_tag.get(
    'value', '') for input_tag in form1.find_all('input')}

# Enviar la solicitud POST con la sesión
response = session.post(URL, data=payload, verify=False)

# Crear la ruta completa al archivo T_MASTER_CORD.zip en el directorio del script
zip_file_path = os.path.join(script_directory, "T_MASTER_CORD.zip")

# Guardar el archivo T_MASTER_CORD.zip
with open(zip_file_path, "wb") as zip_file:
    zip_file.write(response.content)

# Ruta al directorio donde quieres extraer los archivos
extracted_dir_path = script_directory

try:
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        # Extraer todo el contenido del ZIP al directorio actual
        zip_ref.extractall(extracted_dir_path)
        print("Contenido del archivo ZIP extraído correctamente en:",
              extracted_dir_path)
except zipfile.BadZipFile:
    print("El archivo descargado no es un archivo ZIP válido.")
