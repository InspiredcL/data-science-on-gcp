# 4. Streaming data: publication and ingest

### Batch processing transformation in DataFlow
* Setup:
    ```
	cd transform; ./install_packages.sh
    ```
* Parsing airports data:
	```
	./df01.py
	head extracted_airports-00000*
	rm extracted_airports-*

 	import apache_beam as beam
	import csv

	if __name__ == '__main__':
   	 with beam.Pipeline('DirectRunner') as pipeline:
       	 airports = (pipeline
                    | beam.io.ReadFromText('airports.csv.gz')
                    | beam.Map(lambda line: next(csv.reader([line])))
                    | beam.Map(lambda fields: (fields[0], (fields[21], fields[26])))
                    )

        (airports
         | beam.Map(lambda airport_data: '{},{}'.format(airport_data[0], ','.join(airport_data[1])))
         | beam.io.WriteToText('extracted_airports')
         )
	```
* Adding timezone information:
	```
	./df02.py
	head airports_with_tz-00000*
	rm airports_with_tz-*

 	import apache_beam as beam
	import csv


	def addtimezone(lat, lon):
   	 try:
       	 import timezonefinder
       	 tf = timezonefinder.TimezoneFinder()
       	 tz = tf.timezone_at(lng=float(lon), lat=float(lat))
        	if tz is None:
          	  tz = 'UTC'
        	return lat, lon, tz
    	except ValueError:
        	return lat, lon, 'TIMEZONE'  # header


	if __name__ == '__main__':
  	  with beam.Pipeline('DirectRunner') as pipeline:
      	  airports = (pipeline
                  	  | beam.io.ReadFromText('airports.csv.gz')
                  	  | beam.Filter(lambda line: "United States" in line)
                   	 | beam.Map(lambda line: next(csv.reader([line])))
                   	 | beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
                  	  )

        	airports | beam.Map(lambda f: '{},{}'.format(f[0], ','.join(f[1]))) | beam.io.textio.WriteToText(
            	'airports_with_tz')
	```
* Converting times to UTC:
	```
	./df03.py
	head -3 all_flights-00000*

 	import apache_beam as beam
	import logging
	import csv
	import json
	
	
	# Definición de la función que agrega la zona horaria basada en coordenadas
	def addtimezone(lat, lon):
	    try:
	        # Importar la librería timezonefinder
	        import timezonefinder
	        # Crear una instancia de TimezoneFinder
	        tf = timezonefinder.TimezoneFinder()
	        # Convertir las coordenadas a números de punto flotante
	        lat = float(lat)
	        lon = float(lon)
	        # Devolver las coordenadas y la zona horaria correspondiente
	        return lat, lon, tf.timezone_at(lng=lon, lat=lat)
	    except ValueError:
	        # Manejo de excepción en caso de error de valor
	        return lat, lon, 'TIMEZONE'  # Encabezado
	
	
	
	def as_utc(date, hhmm, tzone):
	    try:
	        # Verifica si hay una hora válida y una zona horaria proporcionada
	        if len(hhmm) > 0 and tzone is not None:
	            # Importa los módulos datetime y pytz para manejar fechas y zonas horarias
	            import datetime, pytz
	            
	            # Crea un objeto de zona horaria utilizando la zona proporcionada
	            loc_tz = pytz.timezone(tzone)
	            
	            # Convierte la fecha en un objeto datetime en la zona horaria local
	            loc_dt = loc_tz.localize(datetime.datetime.strptime(date, '%Y-%m-%d'), is_dst=False)
	            
	            # La hora se divide en horas y minutos, y se agrega a la fecha y hora local
	            loc_dt += datetime.timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
	            
	            # Convierte la fecha y hora local en UTC
	            utc_dt = loc_dt.astimezone(pytz.utc)
	            
	            # Retorna la fecha y hora en formato de cadena 'YYYY-MM-DD HH:MM:SS'
	            return utc_dt.strftime('%Y-%m-%d %H:%M:%S')
	        else:
	            # Si no hay hora válida o zona horaria, retorna una cadena vacía
	            return ''  # Una cadena vacía corresponde a vuelos cancelados
	    except ValueError as e:
	        # Si ocurre un error de ValueError, registra la excepción y vuelve a lanzarla
	        logging.exception('{} {} {}'.format(date, hhmm, tzone))
	        raise e
	
	
	
	# Definición de la función tz_correct que realiza correcciones de zonas horarias.
	def tz_correct(line, airport_timezones):
	    # Cargamos los campos del registro JSON en un diccionario llamado "fields".
	    fields = json.loads(line)
	    try:
	        # Obtenemos el ID del aeropuerto de origen y destino.
	        dep_airport_id = fields["ORIGIN_AIRPORT_SEQ_ID"]
	        arr_airport_id = fields["DEST_AIRPORT_SEQ_ID"]
	        # Obtenemos las zonas horarias de los aeropuertos de origen y destino.
	        dep_timezone = airport_timezones[dep_airport_id][2]
	        arr_timezone = airport_timezones[arr_airport_id][2]
	        
	        # Iteramos sobre ciertos campos de tiempo y los convertimos a UTC.
	        for f in ["CRS_DEP_TIME", "DEP_TIME", "WHEELS_OFF"]:
	            fields[f] = as_utc(fields["FL_DATE"], fields[f], dep_timezone)
	        
	        # Iteramos sobre otros campos de tiempo y los convertimos a UTC.
	        for f in ["WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
	            fields[f] = as_utc(fields["FL_DATE"], fields[f], arr_timezone)
	        
	        # Generamos una cadena JSON con los campos actualizados y la devolvemos.
	        yield json.dumps(fields)
	    except KeyError as e:
	        # En caso de que falte una clave en el diccionario, registramos una excepción.
	        logging.exception(" Ignorando " + line + " porque el aeropuerto no es conocido")
	
	
	if __name__ == '__main__':
	    # Crear un flujo de trabajo de Beam con el modo 'DirectRunner'
	    with beam.Pipeline('DirectRunner') as pipeline:
	        # Leer el archivo 'airports.csv.gz' y filtrar líneas con "United States"
	        airports = (pipeline
	                    | 'airports:read' >> beam.io.ReadFromText('airports.csv.gz')
	                    | beam.Filter(lambda line: "Estados Unidos" in line)
	                    # Mapear cada línea a los campos correspondientes
	                    | 'airports:fields' >> beam.Map(lambda line: next(csv.reader([line])))
	                    # Mapear los campos para agregar la zona horaria
	                    | 'airports:tz' >> beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
	                    )
	
	        # Leer el archivo 'flights_sample.json' y realizar corrección de zona horaria
	        flights = (pipeline
	                   | 'flights:read' >> beam.io.ReadFromText('flights_sample.json')
	                   | 'flights:tzcorr' >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
	                   )
	
	        # Escribir los resultados en un archivo 'all_flights'
	        flights | beam.io.textio.WriteToText('all_flights')
	```
* Correcting dates:
	```
	./df04.py
	head -3 all_flights-00000*
	rm all_flights-*
 	
	import apache_beam as beam
	import logging
	import csv
	import json
	
	
	def addtimezone(lat, lon):
	    try:
	        import timezonefinder
	        tf = timezonefinder.TimezoneFinder()
	        lat = float(lat)
	        lon = float(lon)
	        return lat, lon, tf.timezone_at(lng=lon, lat=lat)
	    except ValueError:
	        return lat, lon, 'TIMEZONE'  # header
	
	
	def as_utc(date, hhmm, tzone):
	    """
	   Returns date corrected for timezone, and the tzoffset
	   """
	    try:
	        if len(hhmm) > 0 and tzone is not None:
	            import datetime, pytz
	            loc_tz = pytz.timezone(tzone)
	            loc_dt = loc_tz.localize(datetime.datetime.strptime(date, '%Y-%m-%d'), is_dst=False)
	            # can't just parse hhmm because the data contains 2400 and the like ...
	            loc_dt += datetime.timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
	            utc_dt = loc_dt.astimezone(pytz.utc)
	            return utc_dt.strftime('%Y-%m-%d %H:%M:%S'), loc_dt.utcoffset().total_seconds()
	        else:
	            return '', 0  # empty string corresponds to canceled flights
	    except ValueError as e:
	        logging.exception('{} {} {}'.format(date, hhmm, tzone))
	        raise e
	
	
	def add_24h_if_before(arrtime, deptime):
	    import datetime
	    if len(arrtime) > 0 and len(deptime) > 0 and arrtime < deptime:
	        adt = datetime.datetime.strptime(arrtime, '%Y-%m-%d %H:%M:%S')
	        adt += datetime.timedelta(hours=24)
	        return adt.strftime('%Y-%m-%d %H:%M:%S')
	    else:
	        return arrtime
	
	
	def tz_correct(line, airport_timezones):
	    fields = json.loads(line)
	    try:
	        # convert all times to UTC
	        dep_airport_id = fields["ORIGIN_AIRPORT_SEQ_ID"]
	        arr_airport_id = fields["DEST_AIRPORT_SEQ_ID"]
	        dep_timezone = airport_timezones[dep_airport_id][2]
	        arr_timezone = airport_timezones[arr_airport_id][2]
	
	        for f in ["CRS_DEP_TIME", "DEP_TIME", "WHEELS_OFF"]:
	            fields[f], deptz = as_utc(fields["FL_DATE"], fields[f], dep_timezone)
	        for f in ["WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
	            fields[f], arrtz = as_utc(fields["FL_DATE"], fields[f], arr_timezone)
	
	        for f in ["WHEELS_OFF", "WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
	            fields[f] = add_24h_if_before(fields[f], fields["DEP_TIME"])
	
	        fields["DEP_AIRPORT_LAT"] = airport_timezones[dep_airport_id][0]
	        fields["DEP_AIRPORT_LON"] = airport_timezones[dep_airport_id][1]
	        fields["DEP_AIRPORT_TZOFFSET"] = deptz
	        fields["ARR_AIRPORT_LAT"] = airport_timezones[arr_airport_id][0]
	        fields["ARR_AIRPORT_LON"] = airport_timezones[arr_airport_id][1]
	        fields["ARR_AIRPORT_TZOFFSET"] = arrtz
	        yield json.dumps(fields)
	    except KeyError as e:
	        logging.exception(" Ignoring " + line + " because airport is not known")
	
	
	if __name__ == '__main__':
	    with beam.Pipeline('DirectRunner') as pipeline:
	        airports = (pipeline
	                    | 'airports:read' >> beam.io.ReadFromText('airports.csv.gz')
	                    | beam.Filter(lambda line: "United States" in line)
	                    | 'airports:fields' >> beam.Map(lambda line: next(csv.reader([line])))
	                    | 'airports:tz' >> beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
	                    )
	
	        flights = (pipeline
	                   | 'flights:read' >> beam.io.ReadFromText('flights_sample.json')
	                   | 'flights:tzcorr' >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
	                   )
	
	        flights | beam.io.textio.WriteToText('all_flights')
	```
* Create events:
	```
	./df05.py
	head -3 all_events-00000*
	rm all_events-*

 	#Código
	import apache_beam as beam
	import logging
	import csv
	import json
	
	DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
	
	
	def addtimezone(lat, lon):
	    try:
	        import timezonefinder
	        tf = timezonefinder.TimezoneFinder()
	        lat = float(lat)
	        lon = float(lon)
	        return lat, lon, tf.timezone_at(lng=lon, lat=lat)
	    except ValueError:
	        return lat, lon, 'TIMEZONE'  # header
	
	
	def as_utc(date, hhmm, tzone):
	    """
	   Returns date corrected for timezone, and the tzoffset
	   """
	    try:
	        if len(hhmm) > 0 and tzone is not None:
	            import datetime, pytz
	            loc_tz = pytz.timezone(tzone)
	            loc_dt = loc_tz.localize(datetime.datetime.strptime(date, '%Y-%m-%d'), is_dst=False)
	            # can't just parse hhmm because the data contains 2400 and the like ...
	            loc_dt += datetime.timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
	            utc_dt = loc_dt.astimezone(pytz.utc)
	            return utc_dt.strftime(DATETIME_FORMAT), loc_dt.utcoffset().total_seconds()
	        else:
	            return '', 0  # empty string corresponds to canceled flights
	    except ValueError as e:
	        logging.exception('{} {} {}'.format(date, hhmm, tzone))
	        raise e
	
	
	def add_24h_if_before(arrtime, deptime):
	    import datetime
	    if len(arrtime) > 0 and len(deptime) > 0 and arrtime < deptime:
	        adt = datetime.datetime.strptime(arrtime, DATETIME_FORMAT)
	        adt += datetime.timedelta(hours=24)
	        return adt.strftime(DATETIME_FORMAT)
	    else:
	        return arrtime
	
	
	def tz_correct(fields, airport_timezones):
	    try:
	        # convert all times to UTC
	        dep_airport_id = fields["ORIGIN_AIRPORT_SEQ_ID"]
	        arr_airport_id = fields["DEST_AIRPORT_SEQ_ID"]
	        dep_timezone = airport_timezones[dep_airport_id][2]
	        arr_timezone = airport_timezones[arr_airport_id][2]
	
	        for f in ["CRS_DEP_TIME", "DEP_TIME", "WHEELS_OFF"]:
	            fields[f], deptz = as_utc(fields["FL_DATE"], fields[f], dep_timezone)
	        for f in ["WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
	            fields[f], arrtz = as_utc(fields["FL_DATE"], fields[f], arr_timezone)
	
	        for f in ["WHEELS_OFF", "WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
	            fields[f] = add_24h_if_before(fields[f], fields["DEP_TIME"])
	
	        fields["DEP_AIRPORT_LAT"] = airport_timezones[dep_airport_id][0]
	        fields["DEP_AIRPORT_LON"] = airport_timezones[dep_airport_id][1]
	        fields["DEP_AIRPORT_TZOFFSET"] = deptz
	        fields["ARR_AIRPORT_LAT"] = airport_timezones[arr_airport_id][0]
	        fields["ARR_AIRPORT_LON"] = airport_timezones[arr_airport_id][1]
	        fields["ARR_AIRPORT_TZOFFSET"] = arrtz
	        yield fields
	    except KeyError as e:
	        logging.exception(f"Ignoring {fields} because airport is not known")
	
	
	def get_next_event(fields):
	    if len(fields["DEP_TIME"]) > 0:
	        event = dict(fields)  # copy
	        event["EVENT_TYPE"] = "departed"
	        event["EVENT_TIME"] = fields["DEP_TIME"]
	        for f in ["TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "ARR_TIME", "ARR_DELAY", "DISTANCE"]:
	            event.pop(f, None)  # not knowable at departure time
	        yield event
	    if len(fields["ARR_TIME"]) > 0:
	        event = dict(fields)
	        event["EVENT_TYPE"] = "arrived"
	        event["EVENT_TIME"] = fields["ARR_TIME"]
	        yield event
	
	
	def run():
	    with beam.Pipeline('DirectRunner') as pipeline:
	        airports = (pipeline
	                    | 'airports:read' >> beam.io.ReadFromText('airports.csv.gz')
	                    | beam.Filter(lambda line: "United States" in line)
	                    | 'airports:fields' >> beam.Map(lambda line: next(csv.reader([line])))
	                    | 'airports:tz' >> beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
	                    )
	
	        flights = (pipeline
	                   | 'flights:read' >> beam.io.ReadFromText('flights_sample.json')
	                   | 'flights:parse' >> beam.Map(lambda line: json.loads(line))
	                   | 'flights:tzcorr' >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
	                   )
	
	        (flights
	         | 'flights:tostring' >> beam.Map(lambda fields: json.dumps(fields))
	         | 'flights:out' >> beam.io.textio.WriteToText('all_flights')
	         )
	
	        events = flights | beam.FlatMap(get_next_event)
	
	        (events
	         | 'events:tostring' >> beam.Map(lambda fields: json.dumps(fields))
	         | 'events:out' >> beam.io.textio.WriteToText('all_events')
	         )
	
	
	if __name__ == '__main__':
	    run()
	```  
* Read/write to Cloud:
	```
    	./stage_airports_file.sh BUCKETNAME
	./df06.py --project PROJECT --bucket BUCKETNAME

 	#Código
	import apache_beam as beam
	import logging
	import csv
	import json
	
	
	DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
	
	
	def addtimezone(lat, lon):
	    try:
	        import timezonefinder
	        tf = timezonefinder.TimezoneFinder()
	        lat = float(lat)
	        lon = float(lon)
	        return lat, lon, tf.timezone_at(lng=lon, lat=lat)
	    except ValueError:
	        return lat, lon, 'TIMEZONE'  # header
	
	
	def as_utc(date, hhmm, tzone):
	    """
	    Returns date corrected for timezone, and the tzoffset
	    """
	    try:
	        if len(hhmm) > 0 and tzone is not None:
	            import datetime, pytz
	            loc_tz = pytz.timezone(tzone)
	            loc_dt = loc_tz.localize(datetime.datetime.strptime(date, '%Y-%m-%d'), is_dst=False)
	            # can't just parse hhmm because the data contains 2400 and the like ...
	            loc_dt += datetime.timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
	            utc_dt = loc_dt.astimezone(pytz.utc)
	            return utc_dt.strftime(DATETIME_FORMAT), loc_dt.utcoffset().total_seconds()
	        else:
	            return '', 0  # empty string corresponds to canceled flights
	    except ValueError as e:
	        logging.exception('{} {} {}'.format(date, hhmm, tzone))
	        raise e
	
	
	def add_24h_if_before(arrtime, deptime):
	    import datetime
	    if len(arrtime) > 0 and len(deptime) > 0 and arrtime < deptime:
	        adt = datetime.datetime.strptime(arrtime, DATETIME_FORMAT)
	        adt += datetime.timedelta(hours=24)
	        return adt.strftime(DATETIME_FORMAT)
	    else:
	        return arrtime
	
	
	def tz_correct(fields, airport_timezones):
	    fields['FL_DATE'] = fields['FL_DATE'].strftime('%Y-%m-%d')  # convert to a string so JSON code works
	    try:
	        # convert all times to UTC
	        dep_airport_id = fields["ORIGIN_AIRPORT_SEQ_ID"]
	        arr_airport_id = fields["DEST_AIRPORT_SEQ_ID"]
	
	        dep_timezone = airport_timezones[dep_airport_id][2]
	        arr_timezone = airport_timezones[arr_airport_id][2]
	
	        for f in ["CRS_DEP_TIME", "DEP_TIME", "WHEELS_OFF"]:
	            fields[f], deptz = as_utc(fields["FL_DATE"], fields[f], dep_timezone)
	        for f in ["WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
	            fields[f], arrtz = as_utc(fields["FL_DATE"], fields[f], arr_timezone)
	
	        for f in ["WHEELS_OFF", "WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
	            fields[f] = add_24h_if_before(fields[f], fields["DEP_TIME"])
	
	        fields["DEP_AIRPORT_LAT"] = airport_timezones[dep_airport_id][0]
	        fields["DEP_AIRPORT_LON"] = airport_timezones[dep_airport_id][1]
	        fields["DEP_AIRPORT_TZOFFSET"] = deptz
	        fields["ARR_AIRPORT_LAT"] = airport_timezones[arr_airport_id][0]
	        fields["ARR_AIRPORT_LON"] = airport_timezones[arr_airport_id][1]
	        fields["ARR_AIRPORT_TZOFFSET"] = arrtz
	        yield fields
	    except KeyError:
	        #logging.exception(f"Ignoring {fields} because airport is not known")
	        pass
	
	    except KeyError:
	        logging.exception("Ignoring field because airport is not known")
	
	
	def get_next_event(fields):
	    if len(fields["DEP_TIME"]) > 0:
	        event = dict(fields)  # copy
	        event["EVENT_TYPE"] = "departed"
	        event["EVENT_TIME"] = fields["DEP_TIME"]
	        for f in ["TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "ARR_TIME", "ARR_DELAY", "DISTANCE"]:
	            event.pop(f, None)  # not knowable at departure time
	        yield event
	    if len(fields["WHEELS_OFF"]) > 0:
	        event = dict(fields)  # copy
	        event["EVENT_TYPE"] = "wheelsoff"
	        event["EVENT_TIME"] = fields["WHEELS_OFF"]
	        for f in ["WHEELS_ON", "TAXI_IN", "ARR_TIME", "ARR_DELAY", "DISTANCE"]:
	            event.pop(f, None)  # not knowable at departure time
	        yield event
	    if len(fields["ARR_TIME"]) > 0:
	        event = dict(fields)
	        event["EVENT_TYPE"] = "arrived"
	        event["EVENT_TIME"] = fields["ARR_TIME"]
	        yield event
	
	
	def create_event_row(fields):
	    featdict = dict(fields)  # copy
	    featdict['EVENT_DATA'] = json.dumps(fields)
	    return featdict
	
	
	def run(project, bucket):
	    argv = [
	        '--project={0}'.format(project),
	        '--staging_location=gs://{0}/flights/staging/'.format(bucket),
	        '--temp_location=gs://{0}/flights/temp/'.format(bucket),
	        '--runner=DirectRunner'
	    ]
	    airports_filename = 'gs://{}/flights/airports/airports.csv.gz'.format(bucket)
	    flights_output = 'gs://{}/flights/tzcorr/all_flights'.format(bucket)
	
	    with beam.Pipeline(argv=argv) as pipeline:
	        airports = (pipeline
	                    | 'airports:read' >> beam.io.ReadFromText(airports_filename)
	                    | beam.Filter(lambda line: "United States" in line)
	                    | 'airports:fields' >> beam.Map(lambda line: next(csv.reader([line])))
	                    | 'airports:tz' >> beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
	                    )
	
	        flights = (pipeline
	                   | 'flights:read' >> beam.io.ReadFromBigQuery(
	                    query='SELECT * FROM dsongcp.flights WHERE rand() < 0.001', use_standard_sql=True)
	                   | 'flights:tzcorr' >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
	                   )
	
	        (flights
	         | 'flights:tostring' >> beam.Map(lambda fields: json.dumps(fields))
	         | 'flights:gcsout' >> beam.io.textio.WriteToText(flights_output)
	         )
	        
	        flights_schema = ','.join([
	            'FL_DATE:date',
	            'UNIQUE_CARRIER:string',
	            'ORIGIN_AIRPORT_SEQ_ID:string',
	            'ORIGIN:string',
	            'DEST_AIRPORT_SEQ_ID:string',
	            'DEST:string',
	            'CRS_DEP_TIME:timestamp',
	            'DEP_TIME:timestamp',
	            'DEP_DELAY:float',
	            'TAXI_OUT:float',
	            'WHEELS_OFF:timestamp',
	            'WHEELS_ON:timestamp',
	            'TAXI_IN:float',
	            'CRS_ARR_TIME:timestamp',
	            'ARR_TIME:timestamp',
	            'ARR_DELAY:float',
	            'CANCELLED:boolean',
	            'DIVERTED:boolean',
	            'DISTANCE:float',
	            'DEP_AIRPORT_LAT:float',
	            'DEP_AIRPORT_LON:float',
	            'DEP_AIRPORT_TZOFFSET:float',
	            'ARR_AIRPORT_LAT:float',
	            'ARR_AIRPORT_LON:float',
	            'ARR_AIRPORT_TZOFFSET:float',
	            'Year:string'])
	        
	        # autodetect on JSON works, but is less reliable
	        #flights_schema = 'SCHEMA_AUTODETECT'
	        
	        (flights 
	         | 'flights:bqout' >> beam.io.WriteToBigQuery(
	                'dsongcp.flights_tzcorr', 
	                schema=flights_schema,
	                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
	                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
	                )
	        )
	        
	        events = flights | beam.FlatMap(get_next_event)
	        events_schema = ','.join([flights_schema, 'EVENT_TYPE:string,EVENT_TIME:timestamp,EVENT_DATA:string'])
	
	        (events
	         | 'events:totablerow' >> beam.Map(lambda fields: create_event_row(fields))
	         | 'events:bqout' >> beam.io.WriteToBigQuery(
	                'dsongcp.flights_simevents', schema=events_schema,
	                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
	                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
	                )
	        )
	        
	if __name__ == '__main__':
	    import argparse
	
	    parser = argparse.ArgumentParser(description='Run pipeline on the cloud')
	    parser.add_argument('-p', '--project', help='Unique project ID', required=True)
	    parser.add_argument('-b', '--bucket', help='Bucket where gs://BUCKET/flights/airports/airports.csv.gz exists',
	                        required=True)
	
	    args = vars(parser.parse_args())
	
	    print("Correcting timestamps and writing to BigQuery dataset")
	
	    run(project=args['project'], bucket=args['bucket'])
	``` 
    Look for new tables in BigQuery (flights_simevents)
* Run on Cloud:
	```
	./df07.py --project PROJECT --bucket BUCKETNAME --region southamerica-west1

	#Código
	import apache_beam as beam
	import logging
	import csv
	import json
	
	DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
	
	
	# Definición de la función que agrega la zona horaria basada en coordenadas
	def addtimezone(lat, lon):
	    try:
	        # Importar la librería timezonefinder
	        import timezonefinder
	        # Crear una instancia de TimezoneFinder
	        tf = timezonefinder.TimezoneFinder()
	        # Convertir las coordenadas a números de punto flotante
	        lat = float(lat)
	        lon = float(lon)
	        # Devolver las coordenadas y la zona horaria correspondiente
	        return lat, lon, tf.timezone_at(lng=lon, lat=lat)
	    except ValueError:
	        # Manejo de excepción en caso de error de valor
	        return lat, lon, 'TIMEZONE'  # Encabezado
	
	
	
	# Definición de la función as_utc que convierte una fecha y hora en formato UTC
	# a la hora corregida para una zona horaria específica.
	
	def as_utc(date, hhmm, tzone):
	    """
	    Returns date corrected for timezone, and the tzoffset
	    """
	    try:
	        # Verificar si se proporcionó la hora y la zona horaria
	        if len(hhmm) > 0 and tzone is not None:
	            import datetime, pytz
	            # Crear un objeto de zona horaria local basado en la zona proporcionada
	            loc_tz = pytz.timezone(tzone)
	            # Crear un objeto de fecha y hora local con la fecha proporcionada
	            loc_dt = loc_tz.localize(datetime.datetime.strptime(date, '%Y-%m-%d'), is_dst=False)
	            # Agregar la diferencia de horas y minutos proporcionada a la fecha y hora local
	            loc_dt += datetime.timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
	            # Convertir la fecha y hora local a UTC
	            utc_dt = loc_dt.astimezone(pytz.utc)
	            # Devolver la fecha y hora en formato UTC y el desplazamiento de la zona horaria
	            return utc_dt.strftime(DATETIME_FORMAT), loc_dt.utcoffset().total_seconds()
	        else:
	            # Si no se proporciona la hora o la zona horaria, devolver valores predeterminados
	            return '', 0  # Una cadena vacía corresponde a vuelos cancelados
	    except ValueError as e:
	        # Manejar excepciones y registrar detalles
	        logging.exception('{} {} {}'.format(date, hhmm, tzone))
	        raise e
	
	
	
	def add_24h_if_before(arrtime, deptime):
	    import datetime
	    if len(arrtime) > 0 and len(deptime) > 0 and arrtime < deptime:
	        adt = datetime.datetime.strptime(arrtime, DATETIME_FORMAT)
	        adt += datetime.timedelta(hours=24)
	        return adt.strftime(DATETIME_FORMAT)
	    else:
	        return arrtime
	
	
	def airport_timezone(airport_id, airport_timezones):
	    if airport_id in airport_timezones:
	        return airport_timezones[airport_id]
	    else:
	        return '37.41', '-92.35', u'America/Chicago'
	
	
	def tz_correct(fields, airport_timezones):
	    fields['FL_DATE'] = fields['FL_DATE'].strftime('%Y-%m-%d')  # convert to a string so JSON code works
	
	    # convert all times to UTC
	    dep_airport_id = fields["ORIGIN_AIRPORT_SEQ_ID"]
	    arr_airport_id = fields["DEST_AIRPORT_SEQ_ID"]
	    fields["DEP_AIRPORT_LAT"], fields["DEP_AIRPORT_LON"], dep_timezone = airport_timezone(dep_airport_id,
	                                                                                          airport_timezones)
	    fields["ARR_AIRPORT_LAT"], fields["ARR_AIRPORT_LON"], arr_timezone = airport_timezone(arr_airport_id,
	                                                                                          airport_timezones)
	
	    for f in ["CRS_DEP_TIME", "DEP_TIME", "WHEELS_OFF"]:
	        fields[f], deptz = as_utc(fields["FL_DATE"], fields[f], dep_timezone)
	    for f in ["WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
	        fields[f], arrtz = as_utc(fields["FL_DATE"], fields[f], arr_timezone)
	
	    for f in ["WHEELS_OFF", "WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
	        fields[f] = add_24h_if_before(fields[f], fields["DEP_TIME"])
	
	    fields["DEP_AIRPORT_TZOFFSET"] = deptz
	    fields["ARR_AIRPORT_TZOFFSET"] = arrtz
	    yield fields
	
	
	def get_next_event(fields):
	    if len(fields["DEP_TIME"]) > 0:
	        event = dict(fields)  # copy
	        event["EVENT_TYPE"] = "departed"
	        event["EVENT_TIME"] = fields["DEP_TIME"]
	        for f in ["TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "ARR_TIME", "ARR_DELAY", "DISTANCE"]:
	            event.pop(f, None)  # not knowable at departure time
	        yield event
	    if len(fields["WHEELS_OFF"]) > 0:
	        event = dict(fields)  # copy
	        event["EVENT_TYPE"] = "wheelsoff"
	        event["EVENT_TIME"] = fields["WHEELS_OFF"]
	        for f in ["WHEELS_ON", "TAXI_IN", "ARR_TIME", "ARR_DELAY", "DISTANCE"]:
	            event.pop(f, None)  # not knowable at departure time
	        yield event
	    if len(fields["ARR_TIME"]) > 0:
	        event = dict(fields)
	        event["EVENT_TYPE"] = "arrived"
	        event["EVENT_TIME"] = fields["ARR_TIME"]
	        yield event
	
	
	def create_event_row(fields):
	    featdict = dict(fields)  # copy
	    featdict['EVENT_DATA'] = json.dumps(fields)
	    return featdict
	
	
	def run(project, bucket, region):
	    argv = [
	        '--project={0}'.format(project),
	        '--job_name=ch04timecorr',
	        '--save_main_session',
	        '--staging_location=gs://{0}/flights/staging/'.format(bucket),
	        '--temp_location=gs://{0}/flights/temp/'.format(bucket),
	        '--setup_file=./setup.py',
	        '--autoscaling_algorithm=THROUGHPUT_BASED',
	        '--max_num_workers=8',
	        '--region={}'.format(region),
	        '--runner=DataflowRunner'
	    ]
	    airports_filename = 'gs://{}/flights/airports/airports.csv.gz'.format(bucket)
	    flights_output = 'gs://{}/flights/tzcorr/all_flights'.format(bucket)
	
	    with beam.Pipeline(argv=argv) as pipeline:
	        airports = (pipeline
	                    | 'airports:read' >> beam.io.ReadFromText(airports_filename)
	                    | 'airports:onlyUSA' >> beam.Filter(lambda line: "United States" in line)
	                    | 'airports:fields' >> beam.Map(lambda line: next(csv.reader([line])))
	                    | 'airports:tz' >> beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
	                    )
	
	        flights = (pipeline
	                   | 'flights:read' >> beam.io.ReadFromBigQuery(
	                    query='SELECT * FROM dsongcp.flights', use_standard_sql=True)
	                   | 'flights:tzcorr' >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
	                   )
	
	        (flights
	         | 'flights:tostring' >> beam.Map(lambda fields: json.dumps(fields))
	         | 'flights:gcsout' >> beam.io.textio.WriteToText(flights_output)
	         )
	
	        flights_schema = ','.join([
	            'FL_DATE:date,UNIQUE_CARRIER:string,ORIGIN_AIRPORT_SEQ_ID:string,ORIGIN:string',
	            'DEST_AIRPORT_SEQ_ID:string,DEST:string,CRS_DEP_TIME:timestamp,DEP_TIME:timestamp',
	            'DEP_DELAY:float,TAXI_OUT:float,WHEELS_OFF:timestamp,WHEELS_ON:timestamp,TAXI_IN:float',
	            'CRS_ARR_TIME:timestamp,ARR_TIME:timestamp,ARR_DELAY:float,CANCELLED:boolean',
	            'DIVERTED:boolean,DISTANCE:float',
	            'DEP_AIRPORT_LAT:float,DEP_AIRPORT_LON:float,DEP_AIRPORT_TZOFFSET:float',
	            'ARR_AIRPORT_LAT:float,ARR_AIRPORT_LON:float,ARR_AIRPORT_TZOFFSET:float'])
	        flights | 'flights:bqout' >> beam.io.WriteToBigQuery(
	            'dsongcp.flights_tzcorr', schema=flights_schema,
	            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
	            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
	        )
	
	        events = flights | beam.FlatMap(get_next_event)
	        events_schema = ','.join([flights_schema, 'EVENT_TYPE:string,EVENT_TIME:timestamp,EVENT_DATA:string'])
	
	        (events
	         | 'events:totablerow' >> beam.Map(lambda fields: create_event_row(fields))
	         | 'events:bqout' >> beam.io.WriteToBigQuery(
	                    'dsongcp.flights_simevents', schema=events_schema,
	                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
	                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
	                )
	         )
	
	
	if __name__ == '__main__':
	    import argparse
	
	    parser = argparse.ArgumentParser(description='Run pipeline on the cloud')
	    parser.add_argument('-p', '--project', help='Unique project ID', required=True)
	    parser.add_argument('-b', '--bucket', help='Bucket where gs://BUCKET/flights/airports/airports.csv.gz exists',
	                        required=True)
	    parser.add_argument('-r', '--region',
	                        help='Region in which to run the Dataflow job. Choose the same region as your bucket.',
	                        required=True)
	
	    args = vars(parser.parse_args())
	
	    print("Correcting timestamps and writing to BigQuery dataset")
	
	    run(project=args['project'], bucket=args['bucket'], region=args['region'])
	``` 
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
 
### Real-time Stream Processing
* In another CloudShell tab, run avg01.py:
	```
	cd realtime
	./avg01.py --project PROJECT --bucket BUCKETNAME --region us-central1
	```
* In about a minute, you can query events from the BigQuery console:
	```
	SELECT * FROM dsongcp.streaming_events
	ORDER BY EVENT_TIME DESC
    LIMIT 5
	```
* Stop avg01.py by hitting Ctrl+C
* Run avg02.py:
	```
	./avg02.py --project PROJECT --bucket BUCKETNAME --region us-central1
	```
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
	./avg03.py --project PROJECT --bucket BUCKETNAME --region us-central1
	```
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

