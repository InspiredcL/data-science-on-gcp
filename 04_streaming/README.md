# 4. Streaming data: publication and ingest

### Batch processing transformation in DataFlow

* Make a table definition for the federated source

	```
  	cd design; ./mktbl.sh
 	```
 	Los cuales ejecutan el siguiente comando:
  	```
 	bq mk --table --external_table_definition=./airport_schemas.json@JSON=gs://data-science-on-gcp/edition2/raw/airports.csv dsongcp.airports_gcs
  	```

  
* Setup:
   
	```
  	cd transform; ./install_packages.sh
 	```
	* upgrade pip
	* pip cache purge
	* upgrade the packets: timezonefinder, pytz, 'apache-beam[gcp]' </li>

	*If this script fails, please try installing it in a virtualenv*
  
	* "virtualenv ~/beam_env"
	* "source ~/beam_env/bin/activate"
	* "./install_packages.sh"
    
* Parsing airports data:
	```
	./df01.py
	head extracted_airports-00000*
	rm extracted_airports-*
	```
	
	* Lee el archivo 'airports.csv.gz'
 	* A las lineas devueltas por ReadFromText, les aplicamos "next(csv.reader([line]))"
  	* Selecconamos las columnas de indices 0, 21 y 26 agrupadas de la siguiente manera (AIRPORT_SEQ_ID,(LATITUDE, LONGITUDE))
 
  	  
  	* Selecciona las columnas, columna 1 y columna 2 y las separa por una coma
  	* Dicha selección se extrae en un archiivo llamado "extracted_airports"
 
* Adding timezone information:
	```
	./df02.py
	head airports_with_tz-00000*
	rm airports_with_tz-*
	```

	* Función que calcula la zona de tiempo dadas las coordenadas
	* Lee el archivo 'airports.csv.gz'
 	* Filtra los elementos de la fila que conciden con "United States"
  	* A las lineas devueltas por ReadFromText, les aplicamos "next(csv.reader([line]))"
  	* *
  	* De las columnas devuelve la tupla (AIRPORT_SEQ_ID, adddtimezone(LATITUDE, LONGITUDE))
  	* Escribe al archivo "airports_wwith_tz"

 
* BigQuery sample.sh

	* Es llamada en el script stage_airports_file </li>
 
	```
 	./bqsample.sh <bucket_name>
 	```
  	* Consulta que crea "dsongcp.flights_sample" a partir de "dsongcp.flights"
  	* Exporta a un archivo json
   	* Crea un archivo llamado "flight_sample.json" en el directorio actual

* Converting times to UTC:
	```
	./df03.py
	head -3 all_flights-00000*
	```

	* Función que calcula la zona de tiempo dadas las coordenadas
 	* Función que calcula la hora en formato UTC
  	* Función que realiza correcciones de zonas horarias
 	* airports : Lee "airports.csv.gz", Filtra "Estados Unidos", Siguiente linea y tupla de (AIRPORT_SEQ_ID, adddtimezone(LATITUDE, LONGITUDE))
	* flights : Lee "flights_sample.json", Aplica FlatMap a tz_correct y beam.pvalue.AsDict(airports) finalmente Escribe al archivo "all_flights"



  
* Correcting dates:
	```
	./df04.py
	head -3 all_flights-00000*
	rm all_flights-*
	```

 	* Función que calcula la zona de tiempo dadas las coordenadas
 	* Función que calcula la hora en formato UTC
  	* Función que agrega 24 horas, si la llegada ocurre antes que el embarque
  	* Función que realiza correcciones de zonas horarias
 	* airports : Lee "airports.csv.gz", Filtra "Estados Unidos", Siguiente linea y tupla de (AIRPORT_SEQ_ID, adddtimezone(LATITUDE, LONGITUDE))
	* flights : Lee "flights_sample.json", Aplica FlatMap a tz_correct y beam.pvalue.AsDict(airports) finalmente Escribe al archivo "all_flights"

* Create events:
	```
	./df05.py
	head -3 all_events-00000*
	rm all_events-*
	```
	* Funciones anteriores
 	* Función que obtiene el siguiente evento dado el campo
  	* Función Run() para encapsular el pipeline y llamarlo de __main__
  	* airports: ...
  	* flights: Lee y aplica "json.load(line)" antes de FlatMap, tostring "json.dumps(field)", se escribe a "all_flights"
  	* events: events = flights | flatmap(get_next_event), se aplica json.dumps(fields) y se escribe a "all_events"
 
* Read/write to Cloud:
	```
 	./stage_airports_file.sh BUCKETNAME
 	```
 	- Copia el archivo con las coordenadas en el bucket de nuestro proyecto
  	- Carga los datos en el archivo airports.csv, en la tabla dsongcp.airports
 	```
	./df06.py --project PROJECT --bucket BUCKETNAME
	```
	* Funciones anteriores
 	* función para crear fila de evento
  	* Función Run() para encapsular el pipeline y llamarlo de __main__
  	* Se agrega a lo anterior, "flights_schema"
  	* se escribe a "dsongcp.flights_tzcorr"
  	* events:  se aplica a flights FlatMap(get_next_event), se agrega "events_schema"
  	* Se crea una nueva fila de evento para finalmente escribir a "dsongcp.flights_simevents"
  	* __main__ : analizador de argumentos, función run()

  
    Look for new tables in BigQuery (flights_simevents)
* Run on Cloud:
	```
	./df07.py --project PROJECT --bucket BUCKETNAME --region southamerica-west1
	```
	
  	* Funciones anteriores
  	* Función Run() para encapsular el pipeline y llamarlo de __main__
	* airports:
  	* flights: lee dsongcp.flights, FlatMap(), json.dumps(fields), escribe a "flights_output", flights_schema, escribe a "dsongcp.flights_tzcorr"
	* events:  se aplica a flights FlatMap(get_next_event), se agrega "events_schema"
  	* Se crea una nueva fila de evento para finalmente escribir a "dsongcp.flights_simevents"
  	* __main__ : analizador de argumentos, función run()
 
* Test the Data:  	  
* Go to the GCP web console and wait for the Dataflow ch04timecorr job to finish. It might take between 30 minutes and 2+ hours depending on the quota associated with your project (you can change the quota by going to https://console.cloud.google.com/iam-admin/quotas).
* Then, navigate to the BigQuery console and type in:
	```
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
        ORDER BY EVENT_TIME ASC
        LIMIT
          5
	```
 
### Simulate event stream
* In CloudShell, run
	```
 	cd simulate
	python3 ./simulate.py --startTime '2015-05-01 00:00:00 UTC' --endTime '2015-05-04 00:00:00 UTC' --speedFactor=30 --project $DEVSHELL_PROJECT_ID
 	```
 
 	* Función publish(), para publicar
 	* Función notify(), la cual consiste en acumular las filas por lotes, publicando y durmienddo hasta que sea necesario publicar otro lote
 	* __main__ : analizador de argumentos, configuracion de bigquery, jitter, query EVENT_TYPE, TIMESTAMP_ADD, EVENT_DATA, create one Pub/Sub notification topic for each type of event, notify about each row in the dataset

 
### Real-time Stream Processing
* In another CloudShell tab, run avg01.py:
	```
	cd realtime
	./avg01.py --project PROJECT --bucket BUCKETNAME --region southamerica-west1
	```

	* Función run() : argumentos, Pipeline: diccionario eventos, para cada tipo de evento asignarle un nombre de tópico, y a los eventos aplicamos ReadFromPubSub luego parseamos, definimos "all_events" y escribimos los resultados aa una tabla de BigQuery
 	* __main__  : analizamos argumentos, run()
  
* In about a minute, you can query events from the BigQuery console:
	```
	SELECT * FROM dsongcp.streaming_events
	ORDER BY EVENT_TIME DESC
    LIMIT 5
	```
* Stop avg01.py by hitting Ctrl+C
* Run avg02.py:
	```
	./avg02.py --project PROJECT --bucket BUCKETNAME --region southamerica-west1
	```

	* Función: calcular estadísticas
 	* Función: por aeropuerto
  	* Función run(): argumentos, Pipeline: eventos, para embarcado arribado leer desde Pub/Sub y analizar con json.loads, definir "all_events", stats como filtrar por aeropuerto, poner una sliding window, agrupar por xxxx y calcular las estadísticas, para así definir los esquemas de las estadísticas para finalmente escribirlo por Bigquery a "dsongcp.streaming_delays"
  	* __main__ : analizador de argumentos, función run()
  
* In about 5 min, you can query from the BigQuery console:
	```
	SELECT * FROM dsongcp.streaming_delays
	ORDER BY END_TIME DESC
    LIMIT 5
	``` 
* Look at how often the data is coming in:
	```
    SELECT END_TIME, num_flights
    FROM dsongcp.streaming_delays
    ORDER BY END_TIME DESC
    LIMIT 5
	``` 
* It's likely that the pipeline will be stuck. You need to run this on Dataflow.
* Stop avg02.py by hitting Ctrl+C
* In BigQuery, truncate the table:
	```
	TRUNCATE TABLE dsongcp.streaming_delays
	``` 
* Run avg03.py:
	```
	./avg03.py --project PROJECT --bucket BUCKETNAME --region southamerica-west1
	```

	* Cambiamos solo --runner=DataflowRunner
 
* Go to the GCP web console in the Dataflow section and monitor the job.
* Once the job starts writing to BigQuery, run this query and save this as a view:
	```
	SELECT * FROM dsongcp.streaming_delays
    WHERE AIRPORT = 'ATL'
    ORDER BY END_TIME DESC
	```
* Create a view of the latest arrival delay by airport:
	```
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
* Follow the steps in the chapter to connect to Data Studio and create a GeoMap.
* Stop the simulation program in CloudShell.
* From the GCP web console, stop the Dataflow streaming pipeline.

