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

## [Opcional] Cargar los datos en PostgreSQL (Consola)

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

- Añade más meses de datos CSV y observa que el rendimiento se degrada.
  Una vez que haya terminado, elimine la instancia de Cloud SQL ya que
  no la necesitará para el resto del libro.

## Creando una vista en BigQuery

- Ejecuta el script `./create_views.sh`, el cual crea la vista flights

  - (Obs.) las vistas en el sandbox expiran automáticamente a los 60 días,
    por ende si sus datos tienen fecha mayor a 60 dias esta se creará vacía

- Calcule la tabla de contingencia para varios umbrales ejecutando el script
  `./contingency.sh`

  - De la tabla podemos calcular diferentes medidas de evaluación para
    nuestro modelo, dentro de los cuales están:

  1. Precisión (Accuracy): se refiere a que tan exacto es un valor medido
     de un conjunto de datos con respecto a sus valores reales, con respecto
     a nuestro problema de clasificación podemos cuantificarla como (TP+TN)/T
     donde T representa el total de observaciones o sea T=TP+FP+TN+FN

  2. Exactitud (Precision - Positive predicted value):
     es que tan cercanas están las medidas una de otra,
     matemáticamente se relaciona con la dispersión del conjunto de valores
     obtenidos alrededor de un valor central, con respecto a nuestro
     problema de clasificación podemos cuantificarla como TP/PP donde PP
     representa los valores predichos que son positivos o sea PP=TP+FP

  3. Tasa de falsos positivos (FPR - Fall out - Type I error):
     también conocido como probabilidad de falsa alarma, es cuantificado
     como FP/N donde N representa a los valores reales negativos o sea
     N=FP+TN por lo tanto FPR=FP/(FP+TN)

  - false_positives /(true_positives + false_positives) AS fpr

  - ROUND((true_positives + true_negatives) / total, 2) AS accuracy,
    ROUND(true_positives /(true_positives + false_positives),2) AS tpr,
    ROUND(true_negatives /(true_negatives + false_negatives),2) AS tnr,

    ROUND(false_negatives /(false_negatives + true_negatives),2) AS fnr,

## Creando un dashboard

Sigue los pasos del texto principal del capítulo para configurar un dashboard
de Looker Studio y crear gráficos.

Crearemos el panel con Looker pero lo haremos a través de la API de looker
studio con el SDK de python

Para esto debemos habilitar la API de looker studio a través del comando

```sh
gcloud services enable datastudio.googleapis.com
```
