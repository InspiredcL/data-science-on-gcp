#!/bin/bash

# Modificamos el archivo para que tenga fechas que puedan ser cargadas
# en la partición
FOLDER="/home/inspired/data-science-on-gcp/02_ingest/raw_data"
YEAR="2023"
MONTH="5"

# Ejecutamos el script
# python3 python_local_bqload_sandbox.py --path_folder $FOLDER --year $YEAR --month $MONTH

python3 python_local_bqload_sandbox.py --path_folder $FOLDER --year $YEAR --month $MONTH --debug