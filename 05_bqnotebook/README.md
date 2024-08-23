# 5. Análisis de datos interactivo

- [5. Análisis de datos interactivo](#5-análisis-de-datos-interactivo)
  - [Ponte al día con las partes anteriores si es necesario](#ponte-al-día-con-las-partes-anteriores-si-es-necesario)
  - [Prueba las consultas](#prueba-las-consultas)

## Ponte al día con las partes anteriores si es necesario

If you didn't go through Chapters 2-4, the simplest way to catch up is to copy data from my bucket:

- Go to the Storage section of the GCP web console and create a new bucket
- Open CloudShell and git clone this repo:

  ```SH
  git clone https://github.com/InspiredcL/data-science-on-gcp
  ```

- Then, run:

  ```SH
  cd data-science-on-gcp/02_ingest
  ./ingest_from_crsbucket bucketname
  ./bqload.sh  (csv-bucket-name) YEAR
  ```

- Run:

  ```SH
  cd ../03_sqlstudio
  ./create_views.sh
  ```

- Run:

  ```SH
  cd ../04_streaming
  ./ingest_from_crsbucket.sh
  ```

## Prueba las consultas

- En BigQuery, consulta la tabla flights_tzcorr creada en la parte 4:

  ```SQL
  SELECT
      ORIGIN,
      AVG(DEP_DELAY) AS dep_delay,
      AVG(ARR_DELAY) AS arr_delay,
      COUNT(ARR_DELAY) AS num_flights
  FROM
      dsongcp.flights_tzcorr
  GROUP BY
      ORIGIN
  ```

- Prueba las otras consultas en el archivo de texto queries.txt en este
  directorio.

- En el menú de navegación ve a Vertex AI, luego en la pestaña Notebooks
  selecciona Workbench.

- Inicia un nuevo notebook gestionado.
  Luego copia las celdas desde <a href="exploration.ipynb">exploration.ipynb</a>
  y ejecuta el código.

  Obs. Los detalles del código están en el notebook.

- Crea la tabla `trainday` en BigQuery y el archivo CSV para su posterior uso.

  Uso: ./create_trainday_table.sh `nombre_del_bucket`

  ```SH
  ./create_trainday_table.sh
  ```

  Obs. podemos usar el script `create_trainday_table_local.py` de manera local
  para crear la tabla y descargarla a nuestro dispositivo.

  - A traves de command line interface (CLI) realizamos las instrucciones
    - A partir del archivo de texto "traind_day_table.txt" genera una consulta
      en bigquery con la opcion --nouse_legacy_sql
    - Extrae desde la tabla "dsongcp.trainday" y guarda en alguna dirección del
      bucket

  La siguiente consulta es la usada para definir la tabla `trainday`:

  ```SQL
  SELECT
      FL_DATE,
      IF(
          ABS(MOD(FARM_FINGERPRINT(CAST(FL_DATE AS STRING)), 100)) < 70,
          'True',
          'False'
      ) AS is_train_day
  FROM
      (
          SELECT DISTINCT
              (FL_DATE) AS FL_DATE
          FROM
              dsongcp.flights_tzcorr
      )
  ORDER BY
      FL_DATE
  ```
