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

2. Tasa de falsos negativos (FNR - Miss rate - Type II error):
   es cuantificada como FN/P donde N representa a los valores reales
   positivos o sea P=FN+TP por lo tanto FNR=FN/(FN+TP)

3. Tasa de falsos positivos (FPR - Fall out - Type I error):
   también conocida como probabilidad de falsa alarma, es cuantificada
   como FP/N donde N representa a los valores reales negativos o sea
   N=FP+TN por lo tanto FPR=FP/(FP+TN)

4. Tasa de falsos descubrimientos (FDR):
   también conocida como tasa de descubrimiento falso, es cuantificada
   como FP/PP donde PP representa a los valores predichos positivos o sea
   PP=FP+TP por lo tanto FPR=FP/(FP+TP)

5. Tasa de falsas omisiones (FOR):
   también conocida como tasa de omisión falsa, es cuantificada
   como FN/PN donde PN representa a los valores predichos negativos o sea
   PN=FN+TN por lo tanto FPR=FN/(FN+TN)

- [Opcional] Exactitud (Precision - Positive predicted value):
  es que tan cercanas están las medidas una de otra,
  matemáticamente se relaciona con la dispersión del conjunto de valores
  obtenidos alrededor de un valor central, con respecto a nuestro
  problema de clasificación podemos cuantificarla como TP/PP donde PP
  representa los valores predichos que son positivos o sea PP=TP+FP

## Creando un dashboard

Sigue los pasos del texto principal del capítulo para configurar un dashboard
de Looker Studio y crear gráficos.

### Crear reporte y seleccionar fuente de datos

- Ve a la pagina de [Looker Studio](https://lookerstudio.google.com/) y
  crea un informe vacío, automáticamente se abrirá un prompt para añadir
  datos y selecciona BigQuery después de eso selecciona tu proyecto,
  dataset y tabla para finalmente dar click en añadir

### Creando gráficos

Pensando en el público objetivo de este reporte construimos los elementos
que creemos que mas pueden representar nuestro modelo actual.

- Borramos la tabla que viene por defecto, presionamos en añadir un gráfico
  en la lista desplegable seleccionamos gráfico de dispersión y hacemos
  click en el lienzo (o dibujamos un rectángulo con el tamaño que
  queremos el gráfico) Obs. las dimensiones y métricas originales son
  aleatorias

- Seleccionamos dimensión `UNIQUE_CARRIER`, métrica x `DEP_DELAY`
  finalmente métrica y `ARR_DELAY`

- Cambiar la función de agregación para ambas métricas a `AVG`

- En la pestaña estilo agregar una linea de tendencia lineal y seleccionar
  mostrar etiquetas de datos para visualizar la aerolínea.

  - Obs. al crear la linea de tendencia podemos también personalizar su
    apariencia cambiando el grosor, estilo y color.

### Agregar controles para usuario

Hasta ahora el gráfico está estático y necesitamos controles para que el
usuario final interactúe con el panel, para esto agregaremos a nuestro panel
la selección de fechas

- Presiona en añadir un control y en la lista desplegable selecciona
  filtro por periodo para luego hacer click en el lienzo

- Haz click en `Selecciona un periodo` en la ventana que se abrirá haz
  click en `Periodo automático` y selecciona al comienzo de la lista
  `Fijo` para luego elegir las fechas de inicio y fin.

  - Obs. en `Propiedades de Filtro por periodo` en la pestaña de configuración
    podemos seleccionar el periodo predeterminado que se usa al abrir la
    página

- En la esquina superior derecha haz click en `Ver` para obtener una
  previsualización de como verán el panel los usuarios finales

  - Podemos observar que la linea de tendencia sugiere fuertemente un modelo
    lineal, si fuésemos a recomendar las cancelaciones de reuniones en
    base a este gráfico estaríamos sugiriendo basados en la tendencia
    (lineal) del retraso de llegada con respecto al retraso de salida,
    que los retraso de salida de mas de 20 minutos nos llevan a retrasos
    de llegada de mas de 15 minutos y por supuesto este no es nuestro
    modelo actual.

### Mostrando proporciones con un gráfico circular

En base al análisis anterior necesitamos explicar la tabla de contingencia
que construimos anteriormente, una de las mejores maneras de mostrar
proporciones es con un gráfico circular (anillo) o uno de columnas apiladas

En este gráfico queremos mostrar la proporción de vuelos que llegaron tarde
frente a los que llegaron a tiempo, por ende necesitamos una dimensión
para poder medir con respecto a la métrica de el número de vuelos, pero
en nuestro conjunto de datos no existe una columna que indique dicho valor.
sin embargo Looker Studio tiene un valor especial llamado `Record Count`
que podemos usar como la métrica, pero el valor que muestra si un vuelo
llegó tarde o no debemos calcularlo como una fórmula.

- Presiona en añadir gráfico y selecciona en la lista desplegable gráfico
  circular o de anillo.

- En la derecha está la pestaña datos seleccionamos `Añadir un campo`
  luego presionamos añadir campo calculado y se abre una pestaña de datos
  donde ingresamos el nombre `IS_LATE`, Tipo de dato `Texto` y fórmula
  del campo a crear, para finalmente presionar aplicar

  ```sql
  CASE
    WHEN ARR_DELAY < 15 THEN "ON TIME"
    ELSE "LATE"
  END
  ```

- En el gráfico y su pestaña de configuración seleccionamos `FL_DATE`
  como la dimension del periodo, `IS_LATE` como dimensión, `Record Count`
  como métrica y finalmente ordenamos por `Record Count`

### Información extra en un gráfico de columnas

Aunque nuestro gráfico circular nos muestra la proporción de vuelos que
llegan a tiempo versus los que llegan tarde y esta será nuestra base para
tomar decisiones, no le dice al usuario cual será el retraso promedio,
por ende debemos agregar mas información para el usuario la cual permita
visualizar dichos datos, para esto crearemos un gráfico de columnas apiladas.

- Presiona en añadir gráfico y selecciona en la lista desplegable gráfico
  de columnas apiladas.

- En el gráfico y su pestaña de configuración seleccionamos `FL_DATE`
  como la dimension del periodo, `UNIQUE_CARRIER` como dimensión,
  `DEP_DELAY` como primera métrica, `ARR_DELAY` como segunda métrica,
  y finalmente ordenamos por `ARR_DELAY`, en ambas métricas debemos cambiar
  la agregación a la media

- En la pestaña estilo de nuestro gráfico cambiamos el numero de barras
  a 20 y seleccionamos eje único.

### Explicando la tabla de contingencia

Aunque hemos mostrado información útil a los usuarios para la toma de
decisiones aun no consideramos los umbrales que construimos anteriormente
para mostrar nuestro modelo, para esto procedemos a cargar los datos
correspondientes.

- Repetimos los pasos para seleccionar una fuente de datos pero considerando
  las fuentes delayed_10, delayed_15 y delayed_20

- replicamos los pasos para crear los gráficos de barra y de anillo de
  los pasos anteriores seleccionando las fuentes correspondientes

### Conclusión

Si observamos el gráfico circular (anillo) para el umbral de 10 minutos
observamos que se acerca bastante a nuestro objetivo del 30% de las
llegadas puntuales. El gráfico de barras para el umbral de 10 minutos
explica por que es importante dicho umbral, no es sobre el valor exacto
si no que es sobre que indica dicho umbral.

Si bien el retraso de salida promedio ronda los 13 minutos, los vuelos
que sobrepasan los 10 minutos de retraso de salida entran en un régimen
estadístico distinto. El retraso de salida promedio de un avión que sale
con mas de 10 minutos de retraso es mayor a 1 hora.

Una explicación probable es que un vuelo que se retrasa 10 minutos o más
suele tener un problema grave que no se resolverá rápidamente. Si está
sentado en un avión y éste lleva más de 10 minutos de retraso de salida,
es mejor que cancele su reunión, ya que se va a estar en la puerta de
embarque por un rato.

## [Pendiente si es que se puede realizar] - Crear usando API

Crearemos el panel con Looker Studio, pero lo haremos a través de la API
de looker studio.

Para esto debemos habilitar la API de looker studio a través del comando

```sh
gcloud services enable datastudio.googleapis.com
```
