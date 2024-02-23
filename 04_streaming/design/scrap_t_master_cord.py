#!/usr/bin/env python3

""" _summary_

_extended_summary_
"""


import requests
from bs4 import BeautifulSoup

# URL del sitio
URL = "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FLL&QO_fu146_anzr=N8vn6v10+f722146+gnoyr5"

# Realizar la solicitud GET al sitio
response = requests.get(URL)

# Verificar si la solicitud fue exitosa
if response.status_code == 200:
    # Utilizar BeautifulSoup para analizar el HTML
    soup = BeautifulSoup(response.content, 'html.parser')

    # Encontrar el valor del campo "__EVENTVALIDATION"
    event_validation = soup.find(
        'input', {'name': '__EVENTVALIDATION'})['value']

    # Encontrar el valor del campo "__VIEWSTATE"
    view_state = soup.find('input', {'name': '__VIEWSTATE'})['value']

    # Parámetros para la solicitud POST
    params = {
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
        '__EVENTVALIDATION': event_validation,
        '__VIEWSTATE': view_state,
        'chkAllVars': 'on',  # Seleccionar todos los campos
        'btnDownload': 'Download'
    }

    # Realizar la solicitud POST para iniciar la descarga
    download_response = requests.post(URL, data=params)

    # Verificar si la descarga fue exitosa
    if download_response.status_code == 200:
        # Obtener el contenido del archivo zip y guardarlo localmente
        with open('tabla.zip', 'wb') as f:
            f.write(download_response.content)

        print("Descarga completada exitosamente.")
    else:
        print("Error al descargar el archivo.")
else:
    print("Error al acceder al sitio web.")
