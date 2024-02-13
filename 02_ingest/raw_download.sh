#!/bin/bash

# Descarga los archivos zip desde el sitio de la BTS en la carpeta raw_data

# Usage
if test "$#" -ne 4; then
    echo "Usage: ./raw_download.sh start_year start_month end_year end_month"
    echo "   eg: ./raw_download.sh 2022 11 2023 10"
    exit
fi

# Parámetros
START_YEAR=$1
START_MONTH=$2
END_YEAR=$3
END_MONTH=$4

SOURCE=https://transtats.bts.gov/PREZIP

OUTDIR=raw_zipped_data
mkdir -p $OUTDIR

# El flag -k o --insecure, se salta el paso de verificación y procede sin comprobarlo.

for YEAR in $(seq $START_YEAR $END_YEAR); do
    if [[ $YEAR -eq $START_YEAR ]]; then
        # Descargar desde el mes de inicio
        for MONTH in $(seq $START_MONTH 12); do
            FILE=On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${YEAR}_${MONTH}.zip
            curl -k -o ${OUTDIR}/${FILE} ${SOURCE}/${FILE}
        done
    elif [[ $YEAR -eq $END_YEAR ]]; then
        # Descargar hasta el mes de fin
        for MONTH in $(seq 1 $END_MONTH); do
            FILE=On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${YEAR}_${MONTH}.zip
            curl -k -o ${OUTDIR}/${FILE} ${SOURCE}/${FILE}
        done
    else
        # Descargar todos los meses
        for MONTH in $(seq 1 12); do
            FILE=On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${YEAR}_${MONTH}.zip
            curl -k -o ${OUTDIR}/${FILE} ${SOURCE}/${FILE}
        done
    fi
done
