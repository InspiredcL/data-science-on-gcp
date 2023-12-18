# 5. Interactive data exploration

### Catch up from previous chapters if necessary
If you didn't go through Chapters 2-4, the simplest way to catch up is to copy data from my bucket:
* Go to the Storage section of the GCP web console and create a new bucket
* Open CloudShell and git clone this repo:
    ```SH
    git clone https://github.com/GoogleCloudPlatform/data-science-on-gcp
    ```
* Then, run:
    ```SH
    cd data-science-on-gcp/02_ingest
    ./ingest_from_crsbucket bucketname
    ./bqload.sh  (csv-bucket-name) YEAR 
    ```
* Run:
    ```SH
    cd ../03_sqlstudio
    ./create_views.sh
    ```
* Run:
    ```SH
    cd ../04_streaming
    ./ingest_from_crsbucket.sh
    ```

## Try out queries
* In BigQuery, query the time corrected files created in Chapter 4:
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
* Try out the other queries in queries.txt in this directory.

* Navigate to the Vertex AI Workbench part of the GCP console.

* Start a new managed notebook. Then, copy and paste cells from <a href="exploration.ipynb">exploration.ipynb</a> and click Run to execute the code.

* Create the trainday table BigQuery table and CSV file as you will need it later

    Usage: ./create_trainday_table.sh  destination-bucket-name
    ```SH
    ./create_trainday_table.sh
    ```
    * A traves de command line interface realizamos las instrucciones
    : A partir del archivo de texto "traind_day_table.txt" genera una consulta en bigquery con la opcion --nouse_legacy_sql
    : Extrae desde la tabla "dsongcp.trainday" y guarda en alguna dirección del bucket
    
    CREATE TABLE dsongcp.trainday
    ```SQL
    CREATE OR REPLACE TABLE dsongcp.trainday AS
    SELECT
      FL_DATE,
      IF(ABS(MOD(FARM_FINGERPRINT(CAST(FL_DATE AS STRING)), 100)) < 70,
         'True', 'False') AS is_train_day
    FROM (
      SELECT
        DISTINCT(FL_DATE) AS FL_DATE
      FROM
        dsongcp.flights_tzcorr)
    ORDER BY
      FL_DATE
    ```
