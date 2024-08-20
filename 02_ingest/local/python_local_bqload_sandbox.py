#!/usr/bin/env python3

"""
Carga el archivo CSV en el dispositivo local correspondiente al año y
mes indicado a BigQuery, reemplazando los datos existentes en esa partición.
"""

import logging
from google.cloud import bigquery


def bqload(path_folder, year, month):
    """
    Carga el archivo CSV en el dispositivo local a BigQuery, reemplazando
    los datos existentes en esa partición.
    """
    file_path = f"{path_folder}/{year}{month.zfill(2)}.csv"
    client = bigquery.Client()
    # Configurar el trabajo
    table_ref = "bigquery-manu-407202.dsongcp.local_flights_raw"
    print(f"Cargando el mes {month} del año {year} a la tabla: {table_ref}")
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.ignore_unknown_values = True
    job_config.skip_leading_rows = 1
    job_config.schema = [
        bigquery.SchemaField(
            name=col_and_type.split(":")[0],
            field_type=col_and_type.split(":")[1],
            mode="NULLABLE",
        )
        for col_and_type in "Year:STRING,Quarter:STRING,Month:STRING,DayofMonth:STRING,DayOfWeek:STRING,FlightDate:DATE,Reporting_Airline:STRING,DOT_ID_Reporting_Airline:STRING,IATA_CODE_Reporting_Airline:STRING,Tail_Number:STRING,Flight_Number_Reporting_Airline:STRING,OriginAirportID:STRING,OriginAirportSeqID:STRING,OriginCityMarketID:STRING,Origin:STRING,OriginCityName:STRING,OriginState:STRING,OriginStateFips:STRING,OriginStateName:STRING,OriginWac:STRING,DestAirportID:STRING,DestAirportSeqID:STRING,DestCityMarketID:STRING,Dest:STRING,DestCityName:STRING,DestState:STRING,DestStateFips:STRING,DestStateName:STRING,DestWac:STRING,CRSDepTime:STRING,DepTime:STRING,DepDelay:STRING,DepDelayMinutes:STRING,DepDel15:STRING,DepartureDelayGroups:STRING,DepTimeBlk:STRING,TaxiOut:STRING,WheelsOff:STRING,WheelsOn:STRING,TaxiIn:STRING,CRSArrTime:STRING,ArrTime:STRING,ArrDelay:STRING,ArrDelayMinutes:STRING,ArrDel15:STRING,ArrivalDelayGroups:STRING,ArrTimeBlk:STRING,Cancelled:STRING,CancellationCode:STRING,Diverted:STRING,CRSElapsedTime:STRING,ActualElapsedTime:STRING,AirTime:STRING,Flights:STRING,Distance:STRING,DistanceGroup:STRING,CarrierDelay:STRING,WeatherDelay:STRING,NASDelay:STRING,SecurityDelay:STRING,LateAircraftDelay:STRING,FirstDepTime:STRING,TotalAddGTime:STRING,LongestAddGTime:STRING,DivAirportLandings:STRING,DivReachedDest:STRING,DivActualElapsedTime:STRING,DivArrDelay:STRING,DivDistance:STRING,Div1Airport:STRING,Div1AirportID:STRING,Div1AirportSeqID:STRING,Div1WheelsOn:STRING,Div1TotalGTime:STRING,Div1LongestGTime:STRING,Div1WheelsOff:STRING,Div1TailNum:STRING,Div2Airport:STRING,Div2AirportID:STRING,Div2AirportSeqID:STRING,Div2WheelsOn:STRING,Div2TotalGTime:STRING,Div2LongestGTime:STRING,Div2WheelsOff:STRING,Div2TailNum:STRING,Div3Airport:STRING,Div3AirportID:STRING,Div3AirportSeqID:STRING,Div3WheelsOn:STRING,Div3TotalGTime:STRING,Div3LongestGTime:STRING,Div3WheelsOff:STRING,Div3TailNum:STRING,Div4Airport:STRING,Div4AirportID:STRING,Div4AirportSeqID:STRING,Div4WheelsOn:STRING,Div4TotalGTime:STRING,Div4LongestGTime:STRING,Div4WheelsOff:STRING,Div4TailNum:STRING,Div5Airport:STRING,Div5AirportID:STRING,Div5AirportSeqID:STRING,Div5WheelsOn:STRING,Div5TotalGTime:STRING,Div5LongestGTime:STRING,Div5WheelsOff:STRING,Div5TailNum:STRING,last:STRING".split(
            ","
        )
    ]
    with open(file_path, "rb") as source_file:
        load_job = client.load_table_from_file(
            file_obj=source_file, destination=table_ref, job_config=job_config
        )
    load_job.result()  # Espera a que la tabla cargue por completo
    if load_job.state != "DONE":
        # Verificar si funciona el comando load_job.exception()
        # raise ValueError(f"Error al cargar los datos: {load_job.state}")
        raise ValueError(f"Error al cargar los datos: {load_job.exception()}")
    return table_ref, load_job.output_rows, client.get_table(table_ref)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Carga datos de vuelos desde fuente local a \
        BigQuery"
    )
    parser.add_argument(
        "--path_folder", help="Desde donde cargar los datos", required=True
    )
    parser.add_argument("--year", help="Ejemplo: 2015.", required=True)
    parser.add_argument("--month", help="Ejemplo: 1 para Enero.", required=True)
    parser.add_argument(
        "--debug", dest="debug", action="store_true", help="Mensaje de debug"
    )

    try:
        args = parser.parse_args()
        if args.debug:
            logging.basicConfig(
                format="%(levelname)s: %(message)s", level=logging.DEBUG
            )
        else:
            logging.basicConfig(
                format="%(levelname)s: %(message)s", level=logging.INFO
            )
        # usamos los argumentos
        year_ = args.year
        month_ = args.month
        logging.debug("Procesando año=%s y mes=%s", year_, month_)
        tableref, out_rows, destination_table = bqload(
            args.path_folder, year_, month_
        )
        logging.info(
            "Éxito:\n\t Se han procesado %s filas en la tabla %s sumando "
            "un total de %d filas",
            out_rows,
            tableref,
            destination_table.num_rows,
        )
    except Exception as er:
        logging.exception(
            "Error al procesar datos para el año %s y mes %s.\
            Intente nuevamente más tarde o verifique los parámetros de entrada",
            year_,
            month_,
        )
        raise ValueError from er
