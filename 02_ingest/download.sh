#!/bin/bash


# Descarga y descomprime todas las columnas del dataset on-time performance.

# Note que hemos comentado el sitio de la BTS dado que es lenta la descarga y/o el sitio está caído.

# Obs: los archivos del libro están disponibles hasta 2019.

#SOURCE=https://storage.googleapis.com/data-science-on-gcp/edition2/raw

SOURCE=https://transtats.bts.gov/PREZIP

if test "$#" -ne 2; then
    echo "Usage: ./download.sh year month"
    echo "   eg: ./download.sh 2022 11"
    exit
fi

YEAR=$1
MONTH=$2
BASEURL="${SOURCE}/On_Time_Reporting_Carrier_On_Time_Performance_1987_present"
echo "Downloading YEAR=$YEAR ...  MONTH=$MONTH ... from $BASEURL"


MONTH2=$(printf "%02d" $MONTH)

TMPDIR=$(mktemp -d)

ZIPFILE=${TMPDIR}/${YEAR}_${MONTH2}.zip
echo $ZIPFILE

# Si la fuente es el sitio de BTS debemos agregar el flag -k para no solicitar los certificados de seguridad
curl -k -o ${ZIPFILE} ${BASEURL}_${YEAR}_${MONTH}.zip
unzip -d ${TMPDIR} ${ZIPFILE}

mv ${TMPDIR}/*.csv ./${YEAR}${MONTH2}.csv
rm -rf ${TMPDIR}
