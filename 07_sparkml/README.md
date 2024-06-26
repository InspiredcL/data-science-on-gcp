
### Catch up from previous chapters if necessary
If you didn't go through Chapters 2-6, the simplest way to catch up is to copy data from my bucket:

#### Catch up from Chapters 2-5
* Open CloudShell and git clone this repo:
    ```sh
    git clone https://github.com/InspiredcL/data-science-on-gcp
    ```
* Go to the 02_ingest folder of the repo, run the program ./ingest_from_crsbucket.sh and specify your bucket name.
* Go to the 04_streaming folder of the repo, run the program ./ingest_from_crsbucket.sh and specify your bucket name.
* Go to the 05_bqnotebook folder of the repo, run the script to load data into BigQuery:
	```sh
	bash create_trainday.sh <BUCKET>
	```
	BUCKET=$1\
	cat trainday_table.txt | bq query --nouse_legacy_sql\
	bq extract dsongcp.trainday gs://${BUCKET}/flights/trainday.csv

#### [Optional] Catch up from Chapter 6
* Use the instructions in the <a href="../06_dataproc/README.md">Chapter 6 README</a> to:
    * launch a minimal Cloud Dataproc cluster with initialization actions for Jupyter (`./create_cluster.sh BUCKET ZONE`)
	```sh
  	# create cluster \
	gcloud dataproc clusters create ch6cluster \
		--enable-component-gateway \
		--region ${REGION} --zone ${REGION}-a \
		--master-machine-type n1-standard-4 \
		--master-boot-disk-size 500 --num-workers 2 \
		--worker-machine-type n1-standard-4 \
		--worker-boot-disk-size 500 \
		--optional-components JUPYTER --project $PROJECT \
		--initialization-actions=$INSTALL \
		--scopes https://www.googleapis.com/auth/cloud-platform
 	```
* Start a new notebook and in a cell, download a read-only clone of this repository:
    ```bash
    %bash
    git clone https://github.com/InspiredcL/data-science-on-gcp
    rm -rf data-science-on-gcp/.git
    ```
* Browse to data-science-on-gcp/07_sparkml_and_bqml/logistic_regression.ipynb
  and run the cells in the notebook (change the BUCKET appropriately).

# 7. Machine Learning: Logistic regression on Spark

## This Chapter
### Logistic regression using Spark
* Launch a large Dataproc cluster:
    ```sh
    ./create_large_cluster.sh BUCKET ZONE
    ```
    ```sh
    # create cluster
    gcloud dataproc clusters create ch7cluster \
        --enable-component-gateway \
        --region ${REGION} --zone ${REGION}-a \
        --master-machine-type n1-standard-4 \
        --master-boot-disk-size 500 \
        --num-workers 30 --num-secondary-workers 20 \
        --worker-machine-type n1-standard-8 \
        --worker-boot-disk-size 500 \
        --project $PROJECT \
        --scopes https://www.googleapis.com/auth/cloud-platform
    
    gcloud dataproc autoscaling-policies import experiment-policy \
        --source=autoscale.yaml --region=$REGION
    
    gcloud dataproc clusters update ch7cluster \
        --autoscaling-policy=experiment-policy --region=$REGION
    ```

* If it fails with quota issues, get increased quota. If you can't have more quota, 
  reduce the number of workers appropriately.

* Submit a Spark job to run the full dataset (change the BUCKET appropriately).
    ```sh
    ./submit_spark.sh BUCKET logistic.py
    ```
    ```sh
    OUTDIR=gs://$BUCKET/flights/sparkmloutput
	gsutil -m rm -r $OUTDIR
	# submit to existing cluster
	gsutil cp $PYSPARK $OUTDIR/$PYSPARK
	gcloud dataproc jobs submit pyspark \
	     --cluster ch7cluster --region $REGION \
	     $OUTDIR/$PYSPARK \
	     -- \
	     --bucket $BUCKET
	```
  
### Feature engineering
* Submit a Spark job to do experimentation: ```./submit_spark.sh BUCKET experiment.py```\
    ```sh
    OUTDIR=gs://$BUCKET/flights/sparkmloutput
    gsutil -m rm -r $OUTDIR
    # submit to existing cluster
    gsutil cp $PYSPARK $OUTDIR/$PYSPARK
    gcloud dataproc jobs submit pyspark \
        --cluster ch7cluster --region $REGION \
        $OUTDIR/$PYSPARK \
        -- \
        --bucket $BUCKET
    ```

### Cleanup
* Delete the cluster either from the GCP web console or by typing in CloudShell, `../06_dataproc/delete_cluster.sh`\
Delete cluster ejecuta el siguiente comando: `gcloud dataproc clusters delete ch6cluster --region $1`\
Observación: la notacion de directorio relativo dice que vamos al directorio padre del directorio actual y de ahi a la ruta que continua
