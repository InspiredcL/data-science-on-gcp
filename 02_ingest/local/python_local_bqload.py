#!/usr/bin/env python3


"""
Carga el archivo CSV en el dispositivo local a BigQuery, reemplazando
los datos existentes en esa partición.
"""

from google.cloud import bigquery

# Modificamos el archivo para que tenga fechas que puedan ser cargadas
# en la partición
file_path, year, month = (
    "/home/inspired/Descargas/On_Time_Reporting_Carrier_On_Time_Performance_Nov2022_Oct2023/202401.csv", "2024", "01")

client = bigquery.Client()
# Truncar la partición existente
TABLE_REF = f"bigquery-manu-407202.dsongcp.flights_raw${year}{month}"
print(TABLE_REF)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
job_config.ignore_unknown_values = True
job_config.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.MONTH, field="FlightDate"
)
job_config.skip_leading_rows = 1
job_config.schema = [
    bigquery.SchemaField(
        name=col_and_type.split(':')[0],
        field_type=col_and_type.split(':')[1],
        mode="NULLABLE")
    for col_and_type in "Year:STRING,Quarter:STRING,Month:STRING,DayofMonth:STRING,DayOfWeek:STRING,FlightDate:DATE,Reporting_Airline:STRING,DOT_ID_Reporting_Airline:STRING,IATA_CODE_Reporting_Airline:STRING,Tail_Number:STRING,Flight_Number_Reporting_Airline:STRING,OriginAirportID:STRING,OriginAirportSeqID:STRING,OriginCityMarketID:STRING,Origin:STRING,OriginCityName:STRING,OriginState:STRING,OriginStateFips:STRING,OriginStateName:STRING,OriginWac:STRING,DestAirportID:STRING,DestAirportSeqID:STRING,DestCityMarketID:STRING,Dest:STRING,DestCityName:STRING,DestState:STRING,DestStateFips:STRING,DestStateName:STRING,DestWac:STRING,CRSDepTime:STRING,DepTime:STRING,DepDelay:STRING,DepDelayMinutes:STRING,DepDel15:STRING,DepartureDelayGroups:STRING,DepTimeBlk:STRING,TaxiOut:STRING,WheelsOff:STRING,WheelsOn:STRING,TaxiIn:STRING,CRSArrTime:STRING,ArrTime:STRING,ArrDelay:STRING,ArrDelayMinutes:STRING,ArrDel15:STRING,ArrivalDelayGroups:STRING,ArrTimeBlk:STRING,Cancelled:STRING,CancellationCode:STRING,Diverted:STRING,CRSElapsedTime:STRING,ActualElapsedTime:STRING,AirTime:STRING,Flights:STRING,Distance:STRING,DistanceGroup:STRING,CarrierDelay:STRING,WeatherDelay:STRING,NASDelay:STRING,SecurityDelay:STRING,LateAircraftDelay:STRING,FirstDepTime:STRING,TotalAddGTime:STRING,LongestAddGTime:STRING,DivAirportLandings:STRING,DivReachedDest:STRING,DivActualElapsedTime:STRING,DivArrDelay:STRING,DivDistance:STRING,Div1Airport:STRING,Div1AirportID:STRING,Div1AirportSeqID:STRING,Div1WheelsOn:STRING,Div1TotalGTime:STRING,Div1LongestGTime:STRING,Div1WheelsOff:STRING,Div1TailNum:STRING,Div2Airport:STRING,Div2AirportID:STRING,Div2AirportSeqID:STRING,Div2WheelsOn:STRING,Div2TotalGTime:STRING,Div2LongestGTime:STRING,Div2WheelsOff:STRING,Div2TailNum:STRING,Div3Airport:STRING,Div3AirportID:STRING,Div3AirportSeqID:STRING,Div3WheelsOn:STRING,Div3TotalGTime:STRING,Div3LongestGTime:STRING,Div3WheelsOff:STRING,Div3TailNum:STRING,Div4Airport:STRING,Div4AirportID:STRING,Div4AirportSeqID:STRING,Div4WheelsOn:STRING,Div4TotalGTime:STRING,Div4LongestGTime:STRING,Div4WheelsOff:STRING,Div4TailNum:STRING,Div5Airport:STRING,Div5AirportID:STRING,Div5AirportSeqID:STRING,Div5WheelsOn:STRING,Div5TotalGTime:STRING,Div5LongestGTime:STRING,Div5WheelsOff:STRING,Div5TailNum:STRING,last:STRING".split(
        ',')
]

with open(file_path, "rb") as source_file:
    load_job = client.load_table_from_file(
        file_obj=source_file, destination=TABLE_REF, job_config=job_config
    )

load_job.result()  # Espera a que la tabla cargue por completo
if load_job.state != 'DONE':
    # Verificar si funciona el comando load_job.exception()
    # raise ValueError(f"Error al cargar los datos: {load_job.state}")
    raise ValueError(f"Error al cargar los datos: {load_job.exception()}")
# table_ref, load_job.output_rows
destination_table = client.get_table(TABLE_REF)  # Make an API request.
print(f"{destination_table.num_rows} Filas cargadas.")
