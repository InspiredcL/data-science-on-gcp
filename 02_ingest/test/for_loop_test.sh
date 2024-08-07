#!/bin/bash


# Uso
if test "$#" -ne 2 -a "$#" -ne 4; then
    echo "Uso: ./for_loop_test.sh año_inicio mes_inicio [año_fin mes_fin]"
    echo " ej: ./for_loop_test.sh 2022 11"
    echo " ej: ./for_loop_test.sh 2022 11 2023 10"
    exit
fi

# Recibir parámetros
START_YEAR=$1
START_MONTH=$2
END_YEAR=$3
END_MONTH=$4

START_MONTH=$(printf "%02d" $START_MONTH)
END_MONTH=$(printf "%02d" $END_MONTH)


if [ -z "$END_YEAR" ]; then
    # Solo se ingresaron año y mes de inicio, ejecutar solo para ese año y mes
    FILE=On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${START_YEAR}_${START_MONTH}.zip
    echo ${FILE}
else
    for YEAR in $(seq -w $START_YEAR $END_YEAR); do
        if [[ $YEAR -eq $START_YEAR && $YEAR -eq $END_YEAR ]]; then
            # Descargar desde el mes de inicio hasta el mes de fin
            for MONTH in $(seq -w $START_MONTH $END_MONTH); do
                FILE=On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${YEAR}_${MONTH}.zip
                echo ${FILE}
            done
        elif [[ $YEAR -eq $START_YEAR ]]; then
            # Descargar desde el mes de inicio hasta diciembre
            for MONTH in $(seq -w $START_MONTH 12); do
                FILE=On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${YEAR}_${MONTH}.zip
                echo ${FILE}
            done
        elif [[ $YEAR -eq $END_YEAR ]]; then
            # Descargar desde enero hasta el mes de fin
            for MONTH in $(seq -w 1 $END_MONTH); do
                FILE=On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${YEAR}_${MONTH}.zip
                echo ${FILE}
            done
        else
            # Descargar todos los meses
            for MONTH in $(seq -w 1 12); do
                FILE=On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${YEAR}_${MONTH}.zip
                echo ${FILE}
            done
        fi
    done
fi
