# 2. Ingerir datos en la nube a través de CLI (Interfaz de línea de comandos)

## Antes de crear el proyecto

- Asegúrate que tienes una cuenta en google cloud.

  ```sh
  gcloud auth init;
  gcloud auth list
  ```

- Existen maneras de replicar este proyecto sin costo, google asigna 300
  créditos de computo (disponibles por 3 meses) para clientes nuevos solo
  necesitas agregar un método de pago, se recomienda al lector que indague
  sobre limites de cuotas para no incurrir en cargos.

- Ejecuta el siguiente comando para conocer más.

  ```sh
  gcloud cheat-sheet
  ```

- Los nombres están encerrados en [].

## Crear un proyecto en google cloud

- El nombre del proyecto "ds-on-gcp" es una sugerencia, reemplaza el valor de
  [PROJECT] por ds-on-gcp.

  ```sh
  gcloud projects create [PROJECT]
  ```

- Lista los proyectos existentes para ver el "Project ID" el cual es un
  identificador único que no se puede modificar.

  ```sh
  gcloud projects list
  ```

- Establece en tu configuración (Default) el proyecto deseado.

  ```sh
  gcloud config set project NOMBRE_DEL_PROYECTO
  ```

## Crea un bucket

- Un bucket es un contenedor de datos utilizado en servicios de almacenamiento
  en la nube, tienen un nombre único y son accesibles a través de una URI única
  y específica, para poder acceder a los archivos almacenados en ellos desde
  cualquier lugar con conexión a internet.

- Almacenamos la ID del proyecto y el nombre del BUCKET para su uso posterior,
  se utiliza la concatenación del ID del proyecto y una descripción del bucket
  para el nombre del bucket, en este caso utilizamos 'ds-on-gcp'.

  ```sh
  PROJECT=$(gcloud config get-value project)
  ```

  ```sh
  BUCKET=${PROJECT}-dsongcp
  ```

- Definimos la variable REGION, podemos ver las ubicaciones de los buckets
  en el enlace
  [Ubicaciones](https://cloud.google.com/storage/docs/locations#available-locations)

  ```sh
  REGION=southamerica-west1
  ```

- Creamos el BUCKET utilizando las variables anteriores, donde ´gsutil storage
  buckets create´ crea el nuevo bucket, el flag --location especifica la
  ubicación de la región (-l), para luego crear en la ubicación indicada en
  google storage.

  ```sh
  gsutil storage buckets create gs://$BUCKET --location $REGION
  ```

## Añade al almacenamiento (Bucket) los datos necesarios para el proyecto del libro

- Abre CloudShell (o tu entorno local con las herramientas instaladas)
  y clona este repositorio con git:

  ```sh
  git clone https://github.com/InspiredcL/data-science-on-gcp
  ```

- Ve a la carpeta 02_ingest del repositorio

  ```sh
  cd data-science-on-gcp/02_ingest
  ```

- Edita el script `ingest.sh` para reflejar los años que quieres procesar
  (al menos necesitas el año 2015)

- Ejecuta `./ingest.sh "bucketname"` o por ejemplo _"gs://ds-on-gcp-394717-dsongcp/"_

## [Opcional - Producción] Programar descargas mensuales

- Creamos un ambiente virtual, en este caso usaremos virtualenv para crear un
  ambiente llamado dsongcp (opcionalmente podemos anteponer un punto . para
  ocultar el ambiente)

  ```sh
  virtualenv ~/dsongcp
  source ~/dsongcp/bin/activate
  ```

- Ejecuta el comando, para instalar las bibliotecas de google cloud, storage
  y bigquery (técnicamente proporcionan una interfaz para acceder a los
  servicios de google cloud platform por ende se catalogarían como API).

  ```py
  pip3 install google-cloud-storage google-cloud-bigquery
  ```

- Ejecuta el siguiente comando, para utilizar tus propias credenciales de
  usuario para que tu aplicación acceda a una API, esto sirve para ir
  probando el código.

  ```sh
  gcloud auth application-default login
  ```

- Ir a la carpeta monthlyupdate dentro de 02_ingest en la carpeta del repositorio.

  ```sh
  cd monthlyupdate
  ```

- Probamos ingerir un mes usando el script de Python:

  ```sh
  ./ingest_flights.py --bucket your-bucket-name --year 2022 --month 11 --debug
  ```

- Configura una cuenta de servicio llamada svc-monthly-ingest ejecutando:

  ```sh
  ./01_setup_svc_acct.sh
  ```

  **Explicación del script en detalle:**

  - Crea un bucket de Cloud Storage: Si no existe un bucket con el nombre
  especificado, lo crea en la región configurada.

  - Habilita la protección de acceso uniforme a nivel de bucket
  (Uniform Bucket Level Access, UBLA): Esta medida de seguridad garantiza
  que todos los objetos del bucket tengan políticas de acceso explícitas.

  - Crea una cuenta de servicio: Asigna un nombre a la cuenta de servicio
  y la asocia a la función de ingesta de datos.

  - Otorga permisos a la cuenta de servicio:

    - Otorga a la cuenta de servicio el rol de administrador del bucket
    de Cloud Storage, lo que le permite leer, escribir, listar y eliminar
    objetos en el bucket.

    - Otorga a la cuenta de servicio el rol de propietario de datos en
    el esquema BigQuery especificado, lo que le permite crear y eliminar
    particiones en las tablas de BigQuery.

    - Otorga a la cuenta de servicio el rol de usuario de BigQuery, lo
    que le permite ejecutar trabajos de BigQuery.

    - Otorga a la cuenta de servicio el rol de invocador de Cloud Functions,
    lo que le permite invocar funciones de Cloud Functions.

- Ahora, intenta ejecutar el script de importación como una cuenta de servicio:

  **Descripción de la tarea:**

  - Creamos la cuenta de servicio:

    Si es que no ha ejecutado el script anterior o lo ejecuto en otra
    sesión de shell que no tenga las variables locales, ejecuta el primero.

    ```sh
    SVC_ACCT=svc-monthly-ingest; PROJECT_ID=$(gcloud config get-value project)
    ```

    Ahora creamos la nueva clave en formato JSON para la cuenta de servicio
    especificada (formato por default, ya que p12 esta disponible por
    razones de compatibilidad con versiones anteriores), ejecutando los
    sub comandos de gcloud tales como iam, service-accounts, keys

    ```sh
    gcloud iam service-accounts keys create tempkey.json --iam-account=$SVC_ACCT@$PROJECT_ID.iam.gserviceaccount.com --project_id=$PROJECT_ID
    ```

  - Nos autenticamos con la cuenta de servicio usando la llave en formato
    JSON creada anteriormente, asi autorizamos acceso a Google cloud con
    la cuenta de servicio.

    ```sh
    gcloud auth activate-service-account --key-file tempkey.json
    ```

  - Intente ingiriendo un mes de datos.

    ```sh
    ./ingest_flights.py --bucket $BUCKET --year 2022 --month 12 --debug
    ```

  - Vuelve a ejecutar comandos con tus propias credenciales.

    ```sh
    gcloud auth login
    ```

- Ahora podemos desplegar en Cloud Run:

  ```sh
  ./02_deploy_cr.sh
  ```

  **Explicación del script en detalle:**

  - Configuración inicial:

    El script define algunas variables importantes:

    `SERVICE`: Nombre del servicio a desplegar (URL que llama una función
    que se desplegará) (ingest-flights-monthly).

    `SVC_ACCT`: Nombre de la cuenta de servicio que se utilizará
    (svc-monthly-ingest).

    `PROJECT_ID`: ID del proyecto actual de Google Cloud Platform
    (obtenido de la configuración).

    `REGION`: Región donde se desplegará la función (southamerica-west1).

    `SVC_EMAIL`: Email de la cuenta de servicio (combina SVC_ACCT y PROJECT_ID).

  - Cloud Run:

    Se utiliza gcloud run deploy para desplegar un servicio en Cloud Run,
    en este caso una función.

    `--region`: Región donde se desplegará

    `--source=$(pwd)`: Directorio de origen del código de la función
    (directorio actual).

    `--platform=managed`: Plataforma gestionada (sin necesidad de
    configuración manual).

    `--service-account ${SVC_EMAIL}`: Cuenta de servicio asociada a la
    revision del servicio.

    `--no-allow-unauthenticated`: No permite acceso sin autenticación.

    `--timeout`: Establece el tiempo máximo de ejecución de la petición.

- Prueba que puedes invocar la función usando Cloud Run:

  ```sh
  ./03_call_cr.sh
  ```

  **Explicación del script en detalle:**

  - Configura el entorno:

    `SERVICE`: Define el nombre del servicio de Cloud Run (ingest-flights-monthly).

    `PROJECT_ID=$(gcloud config get-value project)`: Obtiene el ID del proyecto GCP usando gcloud config get-value project.

    `BUCKET=${PROJECT_ID}-cf-staging`: Asigna el nombre del bucket de Cloud Storage al valor de la variable BUCKET.

  - Obtiene la URL del servicio:

    `URL=$(gcloud run services describe $SERVICE --format 'value(status.url)')`:

    Utiliza gcloud run services describe para recuperar la URL del servicio y almacenar la URL en la variable URL.

  - Crea un mensaje de datos:

    `echo {\"year\":\"2022\"\,\"month\":\"11\"\,\"bucket\":\"${BUCKET}\"\} >/tmp/message`:

    Crea un archivo temporal llamado /tmp/message que contiene un mensaje JSON con la información del año (2022), mes (11) y bucket ($BUCKET).

  - Envía el mensaje al servicio:

    `curl -k -X POST $URL -H "Authorization: Bearer $(gcloud auth print-identity-token)" -H "Content-Type:application/json" --data-binary @/tmp/message`:

    Utiliza curl para enviar una solicitud POST a la URL del servicio.

    Incluye la cabecera `Authorization` con un token de identidad obtenido de `gcloud auth print-identity-token` asignado al portador de la autorización.

    Especifica la cabecera `Content-Type:application/json` para indicar el formato del mensaje.

    Envía el contenido del archivo `/tmp/message` como datos binarios usando la opción `--data-binary`.

- Prueba la funcionalidad para obtener el mes siguiente:

  ```sh
  ./04_next_month.sh
  ```

  De manera similar, este script define las variables a utilizar, luego obtiene la URL para preguntarse si estamos o no en el último mes y obtener el siguiente (a modo de prueba borra el ultimo mes disponible para poder obtener algo en respuesta) para asi crear el mensaje pero solamente con el nombre del bucket y finalmente realizar la solicitud.

- Configura un trabajo en Cloud Scheduler para invocar a Cloud run cada mes:

  ```sh
  ./05_setup_cron.sh
  ```

  **Explicación del script en detalle:**

  Este script automatiza la ejecución mensual de un servicio Cloud Run llamado ingest-flights-monthly para procesar datos de vuelos:

  - Configura el entorno:

    `JOB`: Nombre del trabajo a crear.

    `SERVICE`: Define el nombre del servicio (ingest-flights-monthly).

    `PROJECT_ID`: Obtiene el ID del proyecto actual usando gcloud config.

    `BUCKET`: Define el bucket de Cloud Storage a utilizar en base al nombre del proyecto.

    `SVC_ACCT`: Define el nombre de la cuenta de servicio (svc-monthly-ingest).

    `SVC_EMAIL`: Construye la dirección de correo electrónico de la cuenta de servicio a partir de los valores anteriores.

    `SVC_URL`: Obtiene la URL del servicio Cloud Run usando gcloud run services describe y el formato específico para extraer sólo la URL.

  - Imprimir información:

    Imprime la URL del servicio `$SVC_URL` y la dirección de correo electrónico de la cuenta de servicio `$SVC_EMAIL`.

  - Preparar mensaje JSON:

    Crea un archivo temporal `/tmp/message` con un mensaje JSON que contiene el bucket a utilizar.

    Imprime el contenido del archivo para visualizarlo (cat /tmp/message).

  - Crear tarea en Cloud Scheduler:

    Utiliza `gcloud scheduler jobs create http` para crear un trabajo de Cloud Scheduler que desencadene una acción a través de HTTP con nombre `$JOB`:

    `--description`: Descripción del trabajo legible por humanos ("Ingest flights using Cloud Run").

    `--schedule`: Horario en el que se ejecutará el trabajo (ejecutar el día 8 de cada mes a las 10:00 AM)

    `--time-zone` Zona horaria de la tarea ejecutada (Santiago de Chile).

    `--uri`: La ruta URI completa a la que se enviará la solicitud ($SVC_URL).

    `--http-method`: Método HTTP a utilizar (POST).

    `--oidc-service-account-email`: El correo electrónico de la cuenta de servicio que se utilizará para generar un token de OpenId Connect que se incluirá en la solicitud enviada al destino al ejecutar el trabajo.

    `--oidc-token-audience`: La Audiencia prevista al generar un token OpenID Connect que será incluido en la solicitud enviada al destino al ejecutar el trabajo.

    `--max-backoff`: La cantidad máxima de tiempo a esperar antes de re-intentar una tarea, después de que dicha tarea falle.

    `--max-retry-attempts`: Número de veces que se debe re-intentar una solicitud si falla o se agota el tiempo de espera.

    `--max-retry-duration`: Límite de tiempo para re-intentar una tarea fallida, medido desde que la tarea se ejecutó por primera vez. Si se especifica con --max-retry-attempts mayor que 0, la tarea se re-intentará hasta que se alcancen ambos límites.

    `--min-backoff`: Cantidad mínima de tiempo que se debe esperar antes de re-intentar una tarea después de que falle.

    `--headers`: Pares de Clave/Valor de encabezados HTTP a incluir en la solicitud.

    `--message-body-from-file`: Ruta al archivo que contiene la carga útil de datos que se incluirá como cuerpo de la petición HTTP. Sólo puede indicarse con métodos HTTP compatibles (PUT o POST).

  - Notas adicionales:

    Para probar este script y ejecutar la tarea debemos darnos la posibilidad de suplantar la cuenta de servicio.

    ```sh
    gcloud scheduler jobs run $JOB --impersonate-service-account
    ```

- Ahora eliminamos la instancia de Cloud run y la tarea programada de Cloud Scheduler ya que no las necesitaremos más para este proyecto. Visit the GCP console for Cloud Run and Cloud Scheduler and delete the Cloud Run instance and the scheduled task—you won’t need them any further.

  ```sh
  gcloud run services delete $SERVICE;
  gcloud scheduler jobs delete $JOB
  ```

## [Opcional] Trabajo manual para crear el script de descarga y revisar los datos

A grandes rasgos primero debemos verificar que los términos de uso del sitio web no le impiden la descarga automatizada, entonces debe usar las herramientas de desarrollador de su navegador para encontrar las llamadas web que hace la forma para la descarga de los archivos.

### Detalles

- El formulario web de la pagina del BTS es un formulario simple sin comportamiento dinámico, este tipo de formulario recopila todas las selecciones en una solicitud POST o GET, si pudiésemos replicar la solicitud en el script podríamos obtener los datos sin rellenar el formulario web.

- Necesitamos saber de donde obtener los datos, para esto en primer lugar entramos en el sitio de la [**BTS**](https://www.transtats.bts.gov), en _Resources_ hacemos click en [**Database directory**](https://www.transtats.bts.gov/DataIndex.asp) el cual nos lleva a **Data Index** y en dicha pagina buscamos el dataset deseado en este caso [**Airline On-time Performance Data**](https://www.transtats.bts.gov/Tables.asp?QO_VQ=EFD&QO_anzr=Nv4yv0r%FDb0-gvzr%FDcr4s14zn0pr%FDQn6n&QO_fu146_anzr=b0-gvzr) para luego hacer click en [**download**](https://www.transtats.bts.gov/DL_SelectFields.asp?gnoyr_VQ=FGJ&QO_fu146_anzr=b0-gvzr) a la derecha de _Reporting Carrier On-Time Performance (1987-present)_ Finalmente marcamos con un click las casillas de Prezipped File, elegimos la fecha (en este caso Enero 2023) y hacemos click en el botón [**Download**](https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2023_1.zip), podemos ver el link de descarga al hacer click con el segundo botón en el archivo a descargar y copiar el link de descarga.

- Para revisar el tipo de solicitud que se realizo en la descarga debemos utilizar las herramientas de desarrollador del navegador para luego ir a red (grabar los registros) repetir los pasos y finalmente observamos que la solicitud es de tipo GET en la dirección del link de descarga.

- Hasta la fecha (2024-01-24) de esta réplica los datos de 2023 están disponibles hasta octubre, por si deseas cambiarlos ya que 2023 fue un año mas normal con respecto a los vuelos (y un año es una cantidad de vuelos razonable).

- Para poder hacer la descarga mas fácil lo ideal es llamar al archivo download.sh y descargar lo necesario, leer los comentarios para satisfacer sus necesidades, dentro de 02_ingest (o donde el usuario estime conveniente) creamos una carpeta para la descarga de los datos y llamamos al siguiente script desde ahi.

- En nuestro caso (deseamos meses de 2 años distintos) podemos hacerlo en 2 partes o crear un script que indique el año/mes de inicio y fin

  ```sh
  START_YEAR=2022
  START_MONTH=11
  END_YEAR=2023
  END_MONTH=10

  for YEAR in $(seq $START_YEAR $END_YEAR); do
      if [[ $YEAR -eq $START_YEAR ]]; then
          # Descargar desde el mes de inicio
          for MONTH in $(seq $START_MONTH 12); do
              bash download.sh $YEAR $MONTH
          done
      else [[ $YEAR -eq $END_YEAR ]]; then
          # Descargar hasta el mes de fin
          for MONTH in $(seq 1 $END_MONTH); do
              bash download.sh $YEAR $MONTH
          done
      fi
  done
  ```
