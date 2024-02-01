# 2. Ingerir datos en la nube a través de CLI (Interfaz de línea de comandos)

## Antes de crear el proyecto

* Asegúrate que tienes una cuenta en google cloud.

* Existen maneras de replicar este proyecto sin costo, google asigna 300 créditos de computo (disponibles por 3 meses) para clientes nuevos solo necesitas agregar un método de pago, se recomienda al lector que indague sobre limites de cuotas para no incurrir en cargos.

* Los nombres están encerrados en [].

## Crear un proyecto en google cloud

* El nombre del proyecto "ds-on-gcp" es una sugerencia, reemplaza el valor de [PROJECT] por ds-on-gcp.  

    ```sh
    gcloud projects create [PROJECT]
    ```
  
* Lista los proyectos existentes para ver el "Project ID" el cual es un identificador único que no se puede modificar.

    ```sh
    gcloud projects list
    ```

* Establece en tu configuración [Default] el proyecto deseado.

    ```sh
    gcloud config set project NOMBRE_DEL_PROYECTO
    ```

## Crea un bucket

* Un bucket es un contenedor de datos utilizado en servicios de almacenamiento en la nube, tienen un nombre único y son accesibles a través de una URI única y específica, para poder acceder a los archivos almacenados en ellos desde cualquier lugar con conexión a internet.

* Almacenamos la ID del proyecto y el nombre del BUCKET para su uso posterior, se utiliza la concatenación del ID del proyecto y una descripción del bucket para el nombre del bucket, en este caso utilizamos 'ds-on-gcp'.

    ```sh
    PROJECT=$(gcloud config get-value project)
    ```

    ```sh
    BUCKET=${PROJECT}-dsongcp
    ```

* Definimos la variable REGION, podemos ver las ubicaciones de los buckets en el enlace [Ubicaciones](https://cloud.google.com/storage/docs/locations#available-locations)

    ```sh
    REGION=southamerica-west1  
    ```

* Creamos el BUCKET utilizando las variables anteriores, donde mb crea el nuevo bucket (make bucket), el flag -l especifica la ubicación de la región (location), para luego crear en la ubicación indicada en google storage.

    ```sh
    gsutil mb -l $REGION gs://$BUCKET
    ```

## Añade al almacenamiento (Bucket) los datos necesarios para el proyecto del libro

* Abre CloudShell (o tu entorno local con las herramientas instaladas) y clona este repositorio con git:

    ```sh
    git clone https://github.com/InspiredcL/data-science-on-gcp
    ```

* Ve a la carpeta 02_ingest del repositorio

    ```sh
    cd data-science-on-gcp/02_ingest
    ```

* Edita el script `ingest.sh` para reflejar los años que quieres procesar (al menos necesitas el año 2015)

* Ejecuta `./ingest.sh "bucketname"` o por ejemplo *"gs://ds-on-gcp-394717-dsongcp/"*

## [Opcional] Programar descargas mensuales Scheduling monthly downloads

* Ir a la carpeta monthlyupdate dentro de 02_ingest en la carpeta del repositorio.

    ```sh
    cd monthlyupdate
    ```

* Ejecuta el comando, para instalar las bibliotecas de google cloud, storage y bigquery (técnicamente proporcionan una interfaz para acceder a los servicios de google cloud platform por ende se catalogarían como API).

    ```py
    pip3 install google-cloud-storage google-cloud-bigquery
    ```

* Ejecuta el comando, para obtener credenciales de autenticación que permiten a las aplicaciones locales acceder a los servicios de GCP.

    ```sh
    gcloud auth application-default login
    ```

* Probamos ingerir un mes usando el script de Python:

    ```sh
    ./ingest_flights.py --debug --bucket your-bucket-name --year 2015 --month 02
    ```

* Set up a service account called svc-monthly-ingest by running `./01_setup_svc_acct.sh`

* Now, try running the ingest script as the service account:

  * Visit the Service Accounts section of the GCP Console: [Consola GCP](https://console.cloud.google.com/iam-admin/serviceaccounts)

  * Select the newly created service account svc-monthly-ingest and click Manage Keys.

  * Add key (Create a new JSON key) and download it to a file named tempkey.json

  * Run `gcloud auth activate-service-account --key-file tempkey.json`

  * Try ingesting one month `./ingest_flights.py --bucket $BUCKET --year 2015 --month 03 --debug`

  * Go back to running command as yourself using `gcloud auth login`

* Deploy to Cloud Run: `./02_deploy_cr.sh`

* Test that you can invoke the function using Cloud Run: `./03_call_cr.sh`

* Test that the functionality to get the next month works: `./04_next_month.sh`

* Set up a Cloud Scheduler job to invoke Cloud Run every month: `./05_setup_cron.sh`

* Visit the GCP Console for Cloud Run and Cloud Scheduler and delete the Cloud Run instance and the scheduled task—you won’t need them any further.

## [Opcional] Trabajo manual para crear el script y revisar los datos

A grandes rasgos primero debemos verificar que los términos de uso del sitio web no le impiden la descarga automatizada, entonces debe usar las herramientas de desarrollador de su navegador para encontrar las llamadas web que hace la forma para la descarga de los archivos.

### Detalles

* El formulario web de la pagina del BTS es un formulario simple sin comportamiento dinámico, este tipo de formulario recopila todas las selecciones en una solicitud POST o GET, si pudiésemos replicar la solicitud en el script podríamos obtener los datos sin rellenar el formulario web.

* Necesitamos saber de donde obtener los datos, para esto en primer lugar entramos en el sitio de la [**BTS**](https://www.transtats.bts.gov), en *Resources* hacemos click en [**Database directory**](https://www.transtats.bts.gov/DataIndex.asp) el cual nos lleva a **Data Index** y en dicha pagina buscamos el dataset deseado en este caso [**Airline On-time Performance Data**](https://www.transtats.bts.gov/Tables.asp?QO_VQ=EFD&QO_anzr=Nv4yv0r%FDb0-gvzr%FDcr4s14zn0pr%FDQn6n&QO_fu146_anzr=b0-gvzr) para luego hacer click en [**download**](https://www.transtats.bts.gov/DL_SelectFields.asp?gnoyr_VQ=FGJ&QO_fu146_anzr=b0-gvzr) a la derecha de *Reporting Carrier On-Time Performance (1987-present)* Finalmente marcamos con un click las casillas de Prezipped File, elegimos la fecha (en este caso Enero 2023) y hacemos click en el botón [**Download**](https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2023_1.zip), podemos ver el link de descarga al hacer click con el segundo botón en el archivo a descargar y copiar el link de descarga.

* Para revisar el tipo de solicitud que se realizo en la descarga debemos utilizar las herramientas de desarrollador del navegador para luego ir a red (grabar los registros) repetir los pasos y finalmente observamos que la solicitud es de tipo GET en la dirección del link de descarga.

* Hasta la fecha (2024-01-24) de esta réplica los datos de 2023 están disponibles hasta octubre, por si deseas cambiarlos ya que 2023 fue un año mas normal con respecto a los vuelos.

* Para poder hacer la descarga mas fácil lo deal es llamar al archivo download.sh y descargar lo necesario, leer los comentarios para satisfacer sus necesidades, dentro de 02_ingest creamos una carpeta para la descarga de los datos y llamamos al script desde ahi.

    ```sh
    for MONTH in $(seq 1 12)
        do bash ../download.sh 2015 $MONTH
    ```
