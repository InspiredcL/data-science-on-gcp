#!/bin/bash

# Carga archivos locales a BigQuery desde año/mes hasta año/mes

# Uso
if test "$#" -ne 2 -a "$#" -ne 4; then
    echo "Uso: ./local_bqload_sandbox.sh año_inicio mes_inicio [año_fin mes_fin]"
    echo " ej: ./local_bqload_sandbox.sh 2022 11"
    echo " ej: ./local_bqload_sandbox.sh 2022 11 2023 10"
    exit
fi

# Parámetros
START_YEAR=$1
START_MONTH=$2
END_YEAR=$3
END_MONTH=$4
START_MONTH=$(printf "%02d" $START_MONTH)
END_MONTH=$(printf "%02d" $END_MONTH)

SCHEMA=Year:STRING,Quarter:STRING,Month:STRING,DayofMonth:STRING,DayOfWeek:STRING,FlightDate:DATE,Reporting_Airline:STRING,DOT_ID_Reporting_Airline:STRING,IATA_CODE_Reporting_Airline:STRING,Tail_Number:STRING,Flight_Number_Reporting_Airline:STRING,OriginAirportID:STRING,OriginAirportSeqID:STRING,OriginCityMarketID:STRING,Origin:STRING,OriginCityName:STRING,OriginState:STRING,OriginStateFips:STRING,OriginStateName:STRING,OriginWac:STRING,DestAirportID:STRING,DestAirportSeqID:STRING,DestCityMarketID:STRING,Dest:STRING,DestCityName:STRING,DestState:STRING,DestStateFips:STRING,DestStateName:STRING,DestWac:STRING,CRSDepTime:STRING,DepTime:STRING,DepDelay:STRING,DepDelayMinutes:STRING,DepDel15:STRING,DepartureDelayGroups:STRING,DepTimeBlk:STRING,TaxiOut:STRING,WheelsOff:STRING,WheelsOn:STRING,TaxiIn:STRING,CRSArrTime:STRING,ArrTime:STRING,ArrDelay:STRING,ArrDelayMinutes:STRING,ArrDel15:STRING,ArrivalDelayGroups:STRING,ArrTimeBlk:STRING,Cancelled:STRING,CancellationCode:STRING,Diverted:STRING,CRSElapsedTime:STRING,ActualElapsedTime:STRING,AirTime:STRING,Flights:STRING,Distance:STRING,DistanceGroup:STRING,CarrierDelay:STRING,WeatherDelay:STRING,NASDelay:STRING,SecurityDelay:STRING,LateAircraftDelay:STRING,FirstDepTime:STRING,TotalAddGTime:STRING,LongestAddGTime:STRING,DivAirportLandings:STRING,DivReachedDest:STRING,DivActualElapsedTime:STRING,DivArrDelay:STRING,DivDistance:STRING,Div1Airport:STRING,Div1AirportID:STRING,Div1AirportSeqID:STRING,Div1WheelsOn:STRING,Div1TotalGTime:STRING,Div1LongestGTime:STRING,Div1WheelsOff:STRING,Div1TailNum:STRING,Div2Airport:STRING,Div2AirportID:STRING,Div2AirportSeqID:STRING,Div2WheelsOn:STRING,Div2TotalGTime:STRING,Div2LongestGTime:STRING,Div2WheelsOff:STRING,Div2TailNum:STRING,Div3Airport:STRING,Div3AirportID:STRING,Div3AirportSeqID:STRING,Div3WheelsOn:STRING,Div3TotalGTime:STRING,Div3LongestGTime:STRING,Div3WheelsOff:STRING,Div3TailNum:STRING,Div4Airport:STRING,Div4AirportID:STRING,Div4AirportSeqID:STRING,Div4WheelsOn:STRING,Div4TotalGTime:STRING,Div4LongestGTime:STRING,Div4WheelsOff:STRING,Div4TailNum:STRING,Div5Airport:STRING,Div5AirportID:STRING,Div5AirportSeqID:STRING,Div5WheelsOn:STRING,Div5TotalGTime:STRING,Div5LongestGTime:STRING,Div5WheelsOff:STRING,Div5TailNum:STRING

# Creamos variables de proyecto/region
PROJECT=$(gcloud config get project)
FOLDER=/home/inspired/data-science-on-gcp/02_ingest/raw_data/

# Elimina la tabla flights_raw si es que no existe, ignorando errores con el flag --force
# bq --project_id $PROJECT show dsongcp.flights_raw || bq --project_id $PROJECT rm --table --force ${PROJECT}:dsongcp.flights_raw

# Crear el dataset dsongcp, si es que no existe
bq --project_id=$PROJECT show dsongcp || bq --location=southamerica-west1 \
    --synchronous_mode mk --dataset dsongcp

if [ -z "$END_YEAR" ]; then
    # Solo se ingresaron año y mes de inicio, ejecutar solo para ese año y mes
    CSVFILE="${FOLDER}${START_YEAR}${START_MONTH}.csv"
    bq --project_id=$PROJECT \
        --location=southamerica-west1 --synchronous_mode \
        load --source_format=CSV --skip_leading_rows=1 \
        --ignore_unknown_values \
        ${PROJECT}:dsongcp.flights_raw $CSVFILE $SCHEMA
else
    for YEAR in $(seq $START_YEAR $END_YEAR); do
        if [[ $YEAR -eq $START_YEAR && $YEAR -eq $END_YEAR ]]; then
            # Descargar desde el mes de inicio hasta el mes de fin
            for MONTH in $(seq $START_MONTH $END_MONTH); do
                CSVFILE="${FOLDER}${YEAR}${MONTH}.csv"
                bq --project_id=$PROJECT \
                    --location=southamerica-west1 --synchronous_mode \
                    load --source_format=CSV --skip_leading_rows=1 \
                    --ignore_unknown_values \
                    ${PROJECT}:dsongcp.flights_raw $CSVFILE $SCHEMA
            done
        elif [[ $YEAR -eq $START_YEAR ]]; then
            # Descargar desde el mes de inicio hasta diciembre
            for MONTH in $(seq $START_MONTH 12); do
                CSVFILE="${FOLDER}${YEAR}${MONTH}.csv"
                bq --project_id=$PROJECT \
                    --location=southamerica-west1 --synchronous_mode \
                    load --source_format=CSV --skip_leading_rows=1 \
                    --ignore_unknown_values \
                    ${PROJECT}:dsongcp.flights_raw $CSVFILE $SCHEMA
            done
        elif [[ $YEAR -eq $END_YEAR ]]; then
            # Descargar desde enero hasta el mes de fin
            for MONTH in $(seq 1 $END_MONTH); do
                CSVFILE="${FOLDER}${YEAR}${MONTH}.csv"
                bq --project_id=$PROJECT \
                    --location=southamerica-west1 --synchronous_mode \
                    load --source_format=CSV --skip_leading_rows=1 \
                    --ignore_unknown_values \
                    ${PROJECT}:dsongcp.flights_raw $CSVFILE $SCHEMA
            done
        else
            # Descargar todos los meses
            for MONTH in $(seq 1 12); do
                CSVFILE="${FOLDER}${YEAR}${MONTH}.csv"
                bq --project_id=$PROJECT \
                    --location=southamerica-west1 --synchronous_mode \
                    load --source_format=CSV --skip_leading_rows=1 \
                    --ignore_unknown_values \
                    ${PROJECT}:dsongcp.flights_raw $CSVFILE $SCHEMA
            done
        fi
    done
fi
