#!/bin/bash


# Descarga y descomprime todas las columnas del dataset on-time performance.

if test "$#" -ne 2; then
    echo "Usage: ./download.sh year month"
    echo "   eg: ./download.sh 2023 1"
    exit
fi

YEAR=$1
MONTH=$2
MONTH2=$(printf "%02d" $MONTH)

SOURCE=https://transtats.bts.gov/PREZIP
BASEURL="${SOURCE}/On_Time_Reporting_Carrier_On_Time_Performance_1987_present"
echo "Downloading YEAR=$YEAR ...  MONTH=$MONTH ... from $BASEURL"

TMPDIR=$(mktemp -d)
ZIPFILE=${TMPDIR}/${YEAR}_${MONTH2}.zip
echo $ZIPFILE

# Si la fuente es el sitio de BTS debemos agregar el flag -k para no solicitar los certificados de seguridad
curl -k -o ${ZIPFILE} ${BASEURL}_${YEAR}_${MONTH}.zip
unzip -d ${TMPDIR} ${ZIPFILE}

mv ${TMPDIR}/*.csv ./${YEAR}${MONTH2}.csv
# mv ${TMPDIR}/*.html ./${YEAR}${MONTH2}.html
rm -rf ${TMPDIR}
