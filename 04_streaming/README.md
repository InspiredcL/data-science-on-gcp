# 4. Streaming de datos: publicación e ingesta

## Póngase al día hasta el capítulo 3 si es necesario

- Ve a la seccion de almacenamiento de la consola de GCP y crea un nuevo bucket
- Abre CloudShell y clona el siguiente repositorio.

```sh
git clone https://github.com/InspiredcL/data-science-on-gcp
```

- Entonces, ejecuta el script para copiar los archivos desde el bucket del curso:

```sh
cd data-science-on-gcp/02_ingest
./ingest_from_crsbucket bucketname
```

- y luego creamos la vista de bigquery:

```sh
cd ../03_sqlstudio
./create_views.sh
```

## Transformación de procesamiento por lotes en DataFlow

### Definir una tabla para el origen federado (bucket)

```sh
cd design; ./mktbl.sh
```

Los cuales ejecutan el siguiente comando:

```sh
bq mk --table --external_table_definition=./airport_schemas.json@JSON=gs://data-science-on-gcp/edition2/raw/airports.csv dsongcp.airports_gcs
```

### Configuración

```sh
cd transform; ./install_packages.sh
```

- pip install --upgrade pip
- pip cache purge
- pip install 'apache-beam[gcp]' timezonefinder[pytz] timezonefinder[numba]
- pip install --upgrade apache-beam[gcp]

#### Si este script falla, por favor intente instalarlo en un ambiente virtual

```sh
virtualenv ~/beam_env
source ~/beam_env/bin/activate
./install_packages.sh
```

### Análisis de datos de aeropuertos (df01.py)

```sh
./df01.py
head extracted_airports-00000*
rm extracted_airports-00000*
```

- Lee el archivo 'airports.csv.gz'
- A las lineas devueltas por ReadFromText, les aplicamos "next(csv.reader([line]))"
- Seleccionamos las columnas de indices 0, 21 y 26 agrupadas de la siguiente manera (AIRPORT_SEQ_ID,(LATITUDE, LONGITUDE))

- Selecciona las columnas, columna 1 y columna 2 y las separa por una coma
- Dicha selección se extrae en un archiivo llamado "extracted_airports"

### Añadir información de zona horaria (df02.py)

```sh
./df02.py
head airports_with_tz-00000*
rm airports_with_tz-*
```

- Función que calcula la zona de tiempo dadas las coordenadas
- Lee el archivo 'airports.csv.gz'
- Filtra los elementos de la fila que coinciden con "United States"
- A las lineas devueltas por ReadFromText, les aplicamos "next(csv.reader([line]))"
- De las columnas devuelve la tupla (AIRPORT_SEQ_ID, addtimezone(LATITUDE, LONGITUDE))
- Escribe al archivo "airports_with_tz"

BigQuery sample.sh

- Es llamada en el script stage_airports_file

```sh
 ./bqsample.sh <bucket_name>
```

- Consulta que crea "dsongcp.flights_sample" a partir de "dsongcp.flights"
- Exporta a un archivo json
- Crea un archivo llamado "flight_sample.json" en el directorio actual

### Conversión de horas a UTC (df03.py)

```sh
./df03.py
head -3 all_flights-00000*
```

- Función que calcula la zona de tiempo dadas las coordenadas
- Función que calcula la hora en formato UTC
- Función que realiza correcciones de zonas horarias
- airports : Lee "airports.csv.gz", Filtra "Estados Unidos", Siguiente linea y tupla de (AIRPORT_SEQ_ID, adddtimezone(LATITUDE, LONGITUDE))
- flights : Lee "flights_sample.json", Aplica FlatMap a tz_correct y beam.pvalue.AsDict(airports) finalmente Escribe al archivo "all_flights"

### Corrección de fechas (df04.py)

```sh
./df04.py
head -3 all_flights-00000*
rm all_flights-*
```

- Función que calcula la zona de tiempo dadas las coordenadas
- Función que calcula la hora en formato UTC
- Función que agrega 24 horas, si la llegada ocurre antes que el embarque
- Función que realiza correcciones de zonas horarias
- airports : Lee "airports.csv.gz", Filtra "Estados Unidos", Siguiente linea y tupla de (AIRPORT_SEQ_ID, adddtimezone(LATITUDE, LONGITUDE))
- flights : Lee "flights_sample.json", Aplica FlatMap a tz_correct y beam.pvalue.AsDict(airports) finalmente Escribe al archivo "all_flights"

### Crear eventos (df05.py)

```sh
./df05.py
head -3 all_events-00000*
rm all_events-*
```

#### Funciones anteriores (corrección de fechas)

- Función que obtiene el siguiente evento dado el campo
- Función Run() para encapsular el pipeline y llamarlo de **main**
- airports: ...
- flights: Lee y aplica "json.load(line)" antes de FlatMap, tostring "json.dumps(field)", se escribe a "all_flights"
- events: events = flights | flatmap(get_next_event), se aplica json.dumps(fields) y se escribe a "all_events"

### Lectura/escritura en la nube (df06.py)

```sh
./stage_airports_file.sh BUCKETNAME
```

- Copia el archivo con las coordenadas en el bucket de nuestro proyecto
- Carga los datos en el archivo airports.csv, en la tabla dsongcp.airports

```sh
./df06.py --project PROJECT --bucket BUCKETNAME
```

#### Funciones anteriores (crear eventos)

- función para crear fila de evento
- Función Run() para encapsular el pipeline y llamarlo de **main**
- Se agrega a lo anterior, "flights_schema"
- se escribe a "dsongcp.flights_tzcorr"
- events: se aplica a flights FlatMap(get_next_event), se agrega "events_schema"
- Se crea una nueva fila de evento para finalmente escribir a "dsongcp.flights_simevents"
- **main** : analizador de argumentos, función run()

Buscar la nueva tabla en BigQuery (flights_simevents)

### Ejecutar en la nube (df07.py)

```sh
./df07.py --project PROJECT --bucket BUCKETNAME --region southamerica-west1
```

#### Funciones anteriores (lectura/escritura en la nube)

- Función Run() para encapsular el pipeline y llamarlo de **main**
- airports:
  - flights: lee dsongcp.flights, FlatMap(), json.dumps(fields), escribe a "flights_output", flights_schema, escribe a "dsongcp.flights_tzcorr"
- events: se aplica a flights FlatMap(get_next_event), se agrega "events_schema"

  - Se crea una nueva fila de evento para finalmente escribir a la tabla "dsongcp.flights_simevents"
  - **main** : analizador de argumentos, función run()

### Comprobar los datos obtenidos

- Vaya a la consola web de GCP y espere a que finalice el trabajo Dataflow ch04timecorr. Puede tardar entre 30 minutos y más de 2 horas, dependiendo de la cuota asociada a su proyecto (puede cambiar la cuota accediendo a <https://console.cloud.google.com/iam-admin/quotas>).
- A continuación, navegue a la consola de BigQuery y escriba:

```sql
SELECT
    ORIGIN,
    DEP_TIME,
    DEST,
    ARR_TIME,
    ARR_DELAY,
    EVENT_TIME,
    EVENT_TYPE
FROM
    dsongcp.flights_simevents
WHERE
    (DEP_DELAY > 15 and ORIGIN = 'SEA') or
    (ARR_DELAY > 15 and DEST = 'SEA')
ORDER BY
    EVENT_TIME ASC
LIMIT
    5
```

## Simulate event stream

En CloudShell, ejecuta:

```sh
 cd simulate
python3 ./simulate.py --startTime '2015-05-01 00:00:00 UTC' --endTime '2015-05-04 00:00:00 UTC' --speedFactor=30 --project $DEVSHELL_PROJECT_ID
```

- Función publish(), para publicar
- Función notify(), la cual consiste en acumular las filas por lotes, publicando y durmiendo hasta que sea necesario publicar otro lote
- **main** : analizador de argumentos, configuracion de bigquery, jitter, query EVENT_TYPE, TIMESTAMP_ADD, EVENT_DATA, create one Pub/Sub notification topic for each type of event, notify about each row in the dataset

### Real-time Stream Processing

- In another CloudShell tab, run avg01.py:

```SH
cd realtime
./avg01.py --project PROJECT --bucket BUCKETNAME --region southamerica-west1
```

- Función run() : argumentos, Pipeline: diccionario eventos, para cada tipo de evento asignarle un nombre de tópico, y a los eventos aplicamos ReadFromPubSub luego parseamos, definimos "all_events" y escribimos los resultados aa una tabla de BigQuery
- **main** : analizamos argumentos, run()

- In about a minute, you can query events from the BigQuery console:

```SQL
SELECT * FROM dsongcp.streaming_events
ORDER BY EVENT_TIME DESC
   LIMIT 5
```

- Stop avg01.py by hitting Ctrl+C
- Run avg02.py:

```SH
./avg02.py --project PROJECT --bucket BUCKETNAME --region southamerica-west1
```

- Función: calcular estadísticas
- Función: por aeropuerto
- Función run(): argumentos, Pipeline: eventos, para embarcado arribado leer desde Pub/Sub y analizar con json.loads, definir "all_events", stats como filtrar por aeropuerto, poner una sliding window, agrupar por xxxx y calcular las estadísticas, para así definir los esquemas de las estadísticas para finalmente escribirlo por Bigquery a "dsongcp.streaming_delays"
- **main** : analizador de argumentos, función run()

- In about 5 min, you can query from the BigQuery console:

```SQL
SELECT * FROM dsongcp.streaming_delays
ORDER BY END_TIME DESC
   LIMIT 5
```

- Look at how often the data is coming in:

```SQL
   SELECT END_TIME, num_flights
   FROM dsongcp.streaming_delays
   ORDER BY END_TIME DESC
   LIMIT 5
```

- It's likely that the pipeline will be stuck. You need to run this on Dataflow.
- Stop avg02.py by hitting Ctrl+C
- In BigQuery, truncate the table:

```SQL
TRUNCATE TABLE dsongcp.streaming_delays
```

- Run avg03.py:

```SH
./avg03.py --project PROJECT --bucket BUCKETNAME --region southamerica-west1
```

- Cambiamos solo --runner=DataflowRunner

- Go to the GCP web console in the Dataflow section and monitor the job.
- Once the job starts writing to BigQuery, run this query and save this as a view:

```SQL
SELECT * FROM dsongcp.streaming_delays
   WHERE AIRPORT = 'ATL'
   ORDER BY END_TIME DESC
```

- Create a view of the latest arrival delay by airport:

```SQL
   CREATE OR REPLACE VIEW dsongcp.airport_delays AS
   WITH delays AS (
       SELECT d.*, a.LATITUDE, a.LONGITUDE
       FROM dsongcp.streaming_delays d
       JOIN dsongcp.airports a USING(AIRPORT)
       WHERE a.AIRPORT_IS_LATEST = 1
   )

   SELECT
       AIRPORT,
       CONCAT(LATITUDE, ',', LONGITUDE) AS LOCATION,
       ARRAY_AGG(
           STRUCT(AVG_ARR_DELAY, AVG_DEP_DELAY, NUM_FLIGHTS, END_TIME)
           ORDER BY END_TIME DESC LIMIT 1) AS a
   FROM delays
   GROUP BY AIRPORT, LONGITUDE, LATITUDE

```

- Follow the steps in the chapter to connect to Data Studio and create a GeoMap.
- Stop the simulation program in CloudShell.
- From the GCP web console, stop the Dataflow streaming pipeline.
