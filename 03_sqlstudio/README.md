# 3. Diseñando dashboards convincentes

## Ponte al día con el capítulo 2

Si aún no lo ha hecho, cargue los datos sin procesar en un conjunto de
datos de BigQuery:

- Ve a la sección Almacenamiento de la consola web de GCP y crea un nuevo bucket
- Abre CloudShell y clona con git este repositorio:

  ```sh
  git clone https://github.com/InspiredcL/data-science-on-gcp
  ```

- Luego, ejecuta:

  ```sh
  cd data-science-on-gcp/02_ingest
  ./ingest.sh bucketname
  ```

## Opcional: Cargar los datos en PostgreSQL

- Vaya a <https://console.cloud.google.com/sql>
- Seleccione crear instancia
- Elija PostgreSQL y, a continuación, rellene el formulario de la siguiente
  manera:
  - Llame a los vuelos de instancia
  - Genere una contraseña segura haciendo clic en GENERAR
  - Elija la versión PostgreSQL por defecto
  - Elija la región en la que se encuentra su cubo de datos CSV
  - Elija una instancia de zona única
  - Elija un tipo de máquina estándar con 2 vCPU
  - Haga clic en Crear instancia
- Tipo (cambie el bucket según sea necesario):

  ```sh
  gsutil cp create_table.sql \
  gs://cloud-training-demos-ml/flights/ch3/create_table.sql
  ```

- Cree una tabla vacía utilizando la consola web:
  - navegue a la sección de bases de datos de Cloud SQL y cree una nueva
  base de datos llamada bts
  - navegue hasta la instancia de vuelos y seleccione IMPORTAR
  - Especifique la ubicación de create_table.sql en su bucket
  - Especifique que desea crear una tabla en la base de datos bts

- Cargue los archivos CSV en esta tabla:
  - Busque 201501.csv en su bucket
  - Especifique CSV como formato
  - bts como base de datos
  - flights como tabla

- En Cloud Shell, conéctese a la base de datos y ejecute las consultas

  - Conéctese a la base de datos utilizando uno de estos dos comandos
  (el primero si no necesita un proxy SQL, el segundo si lo necesita --
  normalmente necesitará un proxy SQL si su organización ha establecido
  una regla de seguridad para permitir el acceso sólo a redes autorizadas):
    1. `gcloud sql connect flights --user=postgres`.
    2. `gcloud beta sql connect flights --user=postgres`.
  - En la línea de comandos, escriba `\c bts;`.
  - Escriba la siguiente consulta:

  ```SQL
  SELECT "Origin", COUNT(*) AS num_flights
  FROM flights GROUP BY "Origin"
  ORDER BY num_flights DESC
  LIMIT 5;
  ```

- Add more months of CSV data and notice that the performance degrades.
  Once you are done, delete the Cloud SQL instance since you will not need it for the rest of the book.

## Creando una vista en BigQuery

- Ejecuta el script `./create_views.sh`
- Calcule la tabla de contingencia para varios umbrales ejecutando el script
  `./contingency.sh`

## Creando un dashboard

Sigue los pasos del texto principal del capítulo para configurar un dashboard
de Looker Studio y crear gráficos.

Crearemos el panel con Looker pero lo haremos a través de la API de looker
studio con el SDK de python

Para esto debemos habilitar la API de looker studio a través del comando

```sh
gcloud services enable datastudio.googleapis.com
```
