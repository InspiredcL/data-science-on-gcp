#!/bin/bash

# Script que descarga datos y los carga a BigQuery

# Verificamos la cantidad de parámetros entregados e indicamos como utilizar el script
if test "$#" -ne 2 -a "$#" -ne 4; then
    echo "Uso: ./ingest_local.sh año_inicio mes_inicio [año_fin mes_fin]"
    echo " ej: ./ingest_local.sh 2022 9"
    echo " ej: ./ingest_local.sh 2022 11 2023 10"
    exit
fi
# Exportamos los parámetros a variables globales
export START_YEAR=$1
export START_MONTH=$2
export END_YEAR=$3
export END_MONTH=$4
START_MONTH2=$(printf "%02d" $START_MONTH)
END_MONTH2=$(printf "%02d" $END_MONTH)


# Obtén los archivos zip desde el sitio de la BTS (), para luego extraerlos.
if [ -z "$END_YEAR" ]; then
    # Solo se ingresaron año y mes de inicio, ejecutar solo para ese año y mes
    bash local_download.sh $START_YEAR $START_MONTH
    bash local_bqload_sandbox.sh $START_YEAR $START_MONTH
    rm ${START_YEAR}${START_MONTH}.csv
else
    for YEAR in $(seq $START_YEAR $END_YEAR); do
        if [[ $YEAR -eq $START_YEAR && $YEAR -eq $END_YEAR ]]; then
            # Descargar desde el mes de inicio hasta el mes de fin
            for MONTH in $(seq $START_MONTH $END_MONTH); do
                bash local_download.sh $YEAR $MONTH
                bash local_bqload_sandbox.sh $YEAR $MONTH
                rm *.csv
            done
        elif [[ $YEAR -eq $START_YEAR ]]; then
            # Descargar desde el mes de inicio hasta diciembre
            for MONTH in $(seq $START_MONTH 12); do
                bash local_download.sh $YEAR $MONTH
                bash local_bqload_sandbox.sh $YEAR $MONTH
                rm *.csv
            done
        elif [[ $YEAR -eq $END_YEAR ]]; then
            # Descargar desde enero hasta el mes de fin
            for MONTH in $(seq 1 $END_MONTH); do
                bash local_download.sh $YEAR $MONTH
                bash local_bqload_sandbox.sh $YEAR $MONTH
                rm *.csv
            done
        else
            # Descargar todos los meses
            for MONTH in $(seq 1 12); do
                bash local_download.sh $YEAR $MONTH
                bash local_bqload_sandbox.sh $YEAR $MONTH
                rm *.csv
            done
        fi
    done
fi

# Consultas de prueba, para verificar que las tablas fueron creadas correctamente
bq query --nouse_legacy_sql \
    'SELECT DISTINCT year, month FROM dsongcp.local_flights_raw ORDER BY year ASC, CAST(month AS INTEGER) ASC'

bq query --nouse_legacy_sql \
    'SELECT year, month, COUNT(*) AS num_flights FROM dsongcp.local_flights_raw GROUP BY year, month ORDER BY year ASC, CAST(month AS INTEGER) ASC'
