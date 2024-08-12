# 4. Streaming de datos: publicación e ingesta

## Ponte al día con el capítulo 3 si es necesario

- Ve a la sección de almacenamiento de la consola de GCP y crea un nuevo bucket
- Abre CloudShell y clona el siguiente repositorio.

```sh
git clone https://github.com/InspiredcL/data-science-on-gcp
```

- Entonces, ejecuta el script para copiar los archivos desde el bucket del
  curso (Obs. los archivos copiados son del año 2015 y enero 2016):

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

El script crea una tabla llamada airports_gcs en bigquery de una
definición externa en este caso el bucket del curso con el esquema del
archivo local airport_schemas.json

#### [Opcional] obtener una versión actualizada de los aeropuertos

Para poder obtener una version actualizada de la tabla airports que
concuerde con nuestros datos podemos descargarla del sitio de la BTS de
manera manual, pero creamos un script para que haga este trabajo

- Ejecuta el script para descargar la tabla y procesarla con
  `python3 ingest_airports_2024.py`

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
- Seleccionamos las columnas de indices 0, 21 y 26 agrupadas de la siguiente
  manera (AIRPORT_SEQ_ID,(LATITUDE, LONGITUDE))
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

En la consola, ejecuta:

<!---
TODO cambiar las fechas para poder replicar en nuestros datos
-->

```sh
PROJECT_ID = $(gcloud config get project)
cd simulate
python3 ./simulate.py --startTime '2015-05-01 00:00:00 UTC' --endTime '2015-05-04 00:00:00 UTC' --speedFactor=30 --project $PROJECT_ID
```

- Función **publish()**, para publicar
- Función **notify()**, la cual consiste en acumular las filas por lotes,
  publicando y durmiendo hasta que sea necesario publicar otro lote
- Función **main** : analizador de argumentos, configuracion de bigquery, jitter,
  query EVENT_TYPE, TIMESTAMP_ADD, EVENT_DATA, create one Pub/Sub notification
  topic for each type of event, notify about each row in the dataset

### Real-time Stream Processing

- In another CloudShell tab, run avg01.py:

```sh
cd realtime
./avg01.py --project PROJECT --bucket BUCKETNAME --region southamerica-west1
```

- Función **run()** : argumentos, Pipeline: diccionario eventos, para
  cada tipo de evento asignarle un nombre de tópico, y a los eventos
  aplicamos ReadFromPubSub luego parseamos, definimos "all_events" y
  escribimos los resultados aa una tabla de BigQuery
- **main** : analizamos argumentos, run()

- En aproximadamente un minuto, podrás consultar los eventos desde la consola
  de BigQuery:

  ```SQL
  SELECT *
  FROM
      dsongcp.streaming_events
  ORDER BY
      EVENT_TIME DESC
  LIMIT
      5
  ```

- Detén avg01.py pulsando Ctrl+C
- Ejecuta avg02.py:

  ```sh
  ./avg02.py --project PROJECT --bucket BUCKETNAME --region southamerica-west1
  ```

- Función: calcular estadísticas
- Función: por aeropuerto
- Función run(): argumentos, Pipeline: eventos, para embarcado arribado leer desde Pub/Sub y analizar con json.loads, definir "all_events", stats como filtrar por aeropuerto, poner una sliding window, agrupar por xxxx y calcular las estadísticas, para así definir los esquemas de las estadísticas para finalmente escribirlo por Bigquery a "dsongcp.streaming_delays"
- **main** : analizador de argumentos, función run()

- En unos 5 minutos podrás realizar consultas desde la consola de BigQuery:

  ```SQL
  SELECT * FROM dsongcp.streaming_delays
  ORDER BY END_TIME DESC
    LIMIT 5
  ```

- Mira con qué frecuencia llegan los datos:

```SQL
   SELECT END_TIME, num_flights
   FROM dsongcp.streaming_delays
   ORDER BY END_TIME DESC
   LIMIT 5
```

- Es probable que el pipeline se atasque. Necesitas ejecutar este código en Dataflow.
- Detén avg02.py pulsando Ctrl+C
- En BigQuery, trunca la tabla:

```sql
TRUNCATE TABLE dsongcp.streaming_delays
```

- Run avg03.py:

```sh
./avg03.py --project PROJECT --bucket BUCKETNAME --region southamerica-west1
```

- Cambiamos solo --runner=DataflowRunner

- En la consola web de GCP ve a la sección Dataflow y monitoriza el trabajo.
- Una vez que el trabajo comience a escribir en BigQuery, ejecuta esta consulta y guárdala como vista:

```SQL
SELECT * FROM dsongcp.streaming_delays
   WHERE AIRPORT = 'ATL'
   ORDER BY END_TIME DESC
```

- Crear una vista del último retraso de llegada por aeropuerto:

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

- Siga los pasos del capítulo para conectarse a Looker Studio y crear un GeoMap.
- Detenga el programa de simulación en CloudShell (o su consola local).
- Desde la consola web de GCP, detenga la canalización de flujo de datos.
