#!/bin/bash


# Creación dataset.tabla = dsongcp.flights_raw duración 60 días

SCHEMA=Year:STRING,Quarter:STRING,Month:STRING,DayofMonth:STRING,DayOfWeek:STRING,FlightDate:DATE,Reporting_Airline:STRING,DOT_ID_Reporting_Airline:STRING,IATA_CODE_Reporting_Airline:STRING,Tail_Number:STRING,Flight_Number_Reporting_Airline:STRING,OriginAirportID:STRING,OriginAirportSeqID:STRING,OriginCityMarketID:STRING,Origin:STRING,OriginCityName:STRING,OriginState:STRING,OriginStateFips:STRING,OriginStateName:STRING,OriginWac:STRING,DestAirportID:STRING,DestAirportSeqID:STRING,DestCityMarketID:STRING,Dest:STRING,DestCityName:STRING,DestState:STRING,DestStateFips:STRING,DestStateName:STRING,DestWac:STRING,CRSDepTime:STRING,DepTime:STRING,DepDelay:STRING,DepDelayMinutes:STRING,DepDel15:STRING,DepartureDelayGroups:STRING,DepTimeBlk:STRING,TaxiOut:STRING,WheelsOff:STRING,WheelsOn:STRING,TaxiIn:STRING,CRSArrTime:STRING,ArrTime:STRING,ArrDelay:STRING,ArrDelayMinutes:STRING,ArrDel15:STRING,ArrivalDelayGroups:STRING,ArrTimeBlk:STRING,Cancelled:STRING,CancellationCode:STRING,Diverted:STRING,CRSElapsedTime:STRING,ActualElapsedTime:STRING,AirTime:STRING,Flights:STRING,Distance:STRING,DistanceGroup:STRING,CarrierDelay:STRING,WeatherDelay:STRING,NASDelay:STRING,SecurityDelay:STRING,LateAircraftDelay:STRING,FirstDepTime:STRING,TotalAddGTime:STRING,LongestAddGTime:STRING,DivAirportLandings:STRING,DivReachedDest:STRING,DivActualElapsedTime:STRING,DivArrDelay:STRING,DivDistance:STRING,Div1Airport:STRING,Div1AirportID:STRING,Div1AirportSeqID:STRING,Div1WheelsOn:STRING,Div1TotalGTime:STRING,Div1LongestGTime:STRING,Div1WheelsOff:STRING,Div1TailNum:STRING,Div2Airport:STRING,Div2AirportID:STRING,Div2AirportSeqID:STRING,Div2WheelsOn:STRING,Div2TotalGTime:STRING,Div2LongestGTime:STRING,Div2WheelsOff:STRING,Div2TailNum:STRING,Div3Airport:STRING,Div3AirportID:STRING,Div3AirportSeqID:STRING,Div3WheelsOn:STRING,Div3TotalGTime:STRING,Div3LongestGTime:STRING,Div3WheelsOff:STRING,Div3TailNum:STRING,Div4Airport:STRING,Div4AirportID:STRING,Div4AirportSeqID:STRING,Div4WheelsOn:STRING,Div4TotalGTime:STRING,Div4LongestGTime:STRING,Div4WheelsOff:STRING,Div4TailNum:STRING,Div5Airport:STRING,Div5AirportID:STRING,Div5AirportSeqID:STRING,Div5WheelsOn:STRING,Div5TotalGTime:STRING,Div5LongestGTime:STRING,Div5WheelsOff:STRING,Div5TailNum:STRING,:STRING

# Creamos variables de proyecto/region
PROJECT=$(gcloud config get project)
REGION=$(gcloud config get compute/region)

# Elimina la tabla flights_raw ignorando errores con el flag -f (-force)
bq --project_id $PROJECT rm -f ${PROJECT}:dsongcp.flights_raw

# Crear el dataset si es que no existe
bq --project_id $PROJECT show dsongcp || bq mk --sync dsongcp

for YEAR in "2022" "2023"; do
    # Definir el rango de meses según el año
    if [ "$YEAR" == "2022" ]; then
        MONTHS="11 12"
    else
        MONTHS="01 02 03 04 05 06 07 08 09 10"
    fi

    # Iterar sobre los meses
    for MONTH in $MONTHS; do
        CSVFILE="/home/inspired/Descargas/On_Time_Reporting_Carrier_On_Time_Performance_Nov2022_Oct2023/${YEAR}${MONTH}.csv"

        bq --project_id=$PROJECT \
            --location=$REGION --synchronous_mode \
            load --time_partitioning_field=FlightDate \
            --time_partitioning_type=MONTH \
            --source_format=CSV --allow_jagged_rows\
            --skip_leading_rows=1 --schema=$SCHEMA \
            ${PROJECT}:dsongcp.flights_raw\$${YEAR}${MONTH} $CSVFILE
    done
done

