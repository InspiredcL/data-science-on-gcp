#!/bin/bash

# Script que descarga por defecto el año 2015, editar dependiendo de su necesidad

# Verificamos la cantidad de parámetros entregados e indicamos como utilizar el script
if [ "$#" -ne 1 ]; then
    echo "Usage: ./ingest.sh  destination-bucket-name"
    exit
fi

# Exportamos el parámetro a la variable global BUCKET
export BUCKET=$1

# Obtén los archivos zip desde el sitio de la BTS (), para luego extraerlos.
for YEAR in $(seq 2015 2015); do
    for MONTH in $(seq 1 12); do
        bash download.sh $YEAR $MONTH
        # Cargar los archivos CSV sin editar a nuestro bucket en GCS
        bash upload.sh $BUCKET
        rm *.csv
    done
    # Carga los archivos CSV en BigQuery como columnas tipo string (especificada en el schema)
    bash bqload.sh $BUCKET $YEAR
done


# Consultas de prueba, para verificar que las tablas fueron creadas correctamente

bq query --nouse_legacy_sql \
    'SELECT DISTINCT year, month FROM dsongcp.flights_raw ORDER BY year ASC, CAST(month AS INTEGER) ASC'

bq query --nouse_legacy_sql \
    'SELECT year, month, COUNT(*) AS num_flights FROM dsongcp.flights_raw GROUP BY year, month ORDER BY year ASC, CAST(month AS INTEGER) ASC'
