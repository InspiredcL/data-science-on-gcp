#!/bin/bash

# Descarga los archivos zip desde el sitio de la BTS en la carpeta raw

#export YEAR=${YEAR:=2015}
SOURCE=https://transtats.bts.gov/PREZIP

OUTDIR=raw
mkdir -p $OUTDIR

# El flag -k o --insecure, se salta el paso de verificación y procede sin comprobarlo.

for YEAR in $(seq 2023 2023); do
    for MONTH in $(seq -w 1 10); do

        FILE=On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${YEAR}_${MONTH}.zip
        curl -k -o ${OUTDIR}/${FILE}  ${SOURCE}/${FILE}

    done
done
