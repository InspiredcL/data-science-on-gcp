### Catch up from previous chapters if necessary
If you didn't go through Chapters 2-9, the simplest way to catch up is to copy data from my bucket:

#### Catch up from Chapters 2-9
* Open CloudShell and git clone this repo:
    ```
    git clone https://github.com/InspiredcL/data-science-on-gcp
    ```
* Go to the 02_ingest folder of the repo, run the program ./ingest_from_crsbucket.sh and specify your bucket name.
* Go to the 04_streaming folder of the repo, run the program ./ingest_from_crsbucket.sh and specify your bucket name.
* Go to the 05_bqnotebook folder of the repo, run the program ./create_trainday.sh and specify your bucket name.
* Go to the 10_mlops folder of the repo, run the program ./ingest_from_crsbucket.sh and specify your bucket name.

# 11. Machine Learning on Streaming Pipelines

#### From CloudShell
* Install the Python libraries you'll need
    ```
    pip3 install google-cloud-aiplatform cloudml-hypertune pyfarmhash
    ```
* [Optional] Create a small, local sample of BigQuery datasets for local experimentation:
    ```bash
    bash create_sample_input.sh
    ```
    
    ```bash
    #!/bin/bash
    bq query --nouse_legacy_sql --format=sparse \
        "SELECT EVENT_DATA FROM dsongcp.flights_simevents \
        WHERE EVENT_TYPE = 'wheelsoff' AND \
        EVENT_TIME BETWEEN '2015-03-10T10:00:00' AND '2015-03-10T14:00:00' " \
        | grep FL_DATE \
        > simevents_sample.json
    
    
    bq query --nouse_legacy_sql --format=json \
        "SELECT * FROM dsongcp.flights_tzcorr \
        WHERE DEP_TIME BETWEEN '2015-03-10T10:00:00' AND '2015-03-10T14:00:00' " \
        | sed 's/\[//g' | sed 's/\]//g' | sed s'/\},/\}\n/g' \
        > alldata_sample.json
    ```
* [Optional] Run a local pipeline to create a training dataset:
    ```
    python3 create_traindata.py --input local
    ```

    ```py
    #!/usr/bin/env python3

    # Copyright 2021 Google Inc.
    #
    # Licensed under the Apache License, Version 2.0 (the "License");
    # you may not use this file except in compliance with the License.
    # You may obtain a copy of the License at
    #
    #     http://www.apache.org/licenses/LICENSE-2.0
    #
    # Unless required by applicable law or agreed to in writing, software
    # distributed under the License is distributed on an "AS IS" BASIS,
    # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    # See the License for the specific language governing permissions and
    # limitations under the License.
    
    import apache_beam as beam
    import logging
    import os
    import json
    
    from flightstxf import flights_transforms as ftxf
    
    CSV_HEADER = 'ontime,dep_delay,taxi_out,distance,origin,dest,dep_hour,is_weekday,carrier,dep_airport_lat,dep_airport_lon,arr_airport_lat,arr_airport_lon,avg_dep_delay,avg_taxi_out,data_split'
    
    
    def dict_to_csv(f):
        try:
            yield ','.join([str(x) for x in f.values()])
        except Exception as e:
            logging.warning('Ignoring {} because: {}'.format(f, e), exc_info=True)
            pass
    
    
    def run(project, bucket, region, input):
        if input == 'local':
            logging.info('Running locally on small extract')
            argv = [
                '--runner=DirectRunner'
            ]
            flights_output = '/tmp/'
        else:
            logging.info('Running in the cloud on full dataset input={}'.format(input))
            argv = [
                '--project={0}'.format(project),
                '--job_name=ch11traindata',
                # '--save_main_session', # not needed as we are running as a package now
                '--staging_location=gs://{0}/flights/staging/'.format(bucket),
                '--temp_location=gs://{0}/flights/temp/'.format(bucket),
                '--setup_file=./setup.py',
                '--autoscaling_algorithm=THROUGHPUT_BASED',
                '--max_num_workers=20',
                # '--max_num_workers=4', '--worker_machine_type=m1-ultramem-40', '--disk_size_gb=500',  # for full 2015-2019 dataset
                '--region={}'.format(region),
                '--runner=DataflowRunner'
            ]
            flights_output = 'gs://{}/ch11/data/'.format(bucket)
    
        with beam.Pipeline(argv=argv) as pipeline:
    
            # read the event stream
            if input == 'local':
                input_file = './alldata_sample.json'
                logging.info("Reading from {} ... Writing to {}".format(input_file, flights_output))
                events = (
                        pipeline
                        | 'read_input' >> beam.io.ReadFromText(input_file)
                        | 'parse_input' >> beam.Map(lambda line: json.loads(line))
                )
            elif input == 'bigquery':
                input_table = 'dsongcp.flights_tzcorr'
                logging.info("Reading from {} ... Writing to {}".format(input_table, flights_output))
                events = (
                        pipeline
                        | 'read_input' >> beam.io.ReadFromBigQuery(table=input_table)
                )
            else:
                logging.error("Unknown input type {}".format(input))
                return
    
            # events -> features.  See ./flights_transforms.py for the code shared between training & prediction
            features = ftxf.transform_events_to_features(events)
    
            # shuffle globally so that we are not at mercy of TensorFlow's shuffle buffer
            features = (
                features
                | 'into_global' >> beam.WindowInto(beam.window.GlobalWindows())
                | 'shuffle' >> beam.util.Reshuffle()
            )
    
            # write out
            for split in ['ALL', 'TRAIN', 'VALIDATE', 'TEST']:
                feats = features
                if split != 'ALL':
                    feats = feats | 'only_{}'.format(split) >> beam.Filter(lambda f: f['data_split'] == split)
                (
                    feats
                    | '{}_to_string'.format(split) >> beam.FlatMap(dict_to_csv)
                    | '{}_to_gcs'.format(split) >> beam.io.textio.WriteToText(os.path.join(flights_output, split.lower()),
                                                                              file_name_suffix='.csv', header=CSV_HEADER,
                                                                              # workaround b/207384805
                                                                              num_shards=1)
                )
    
    
    if __name__ == '__main__':
        import argparse
    
        parser = argparse.ArgumentParser(description='Create training CSV file that includes time-aggregate features')
        parser.add_argument('-p', '--project', help='Project to be billed for Dataflow job. Omit if running locally.')
        parser.add_argument('-b', '--bucket', help='Training data will be written to gs://BUCKET/flights/ch11/')
        parser.add_argument('-r', '--region', help='Region to run Dataflow job. Choose the same region as your bucket.')
        parser.add_argument('-i', '--input', help='local OR bigquery', required=True)
    
        logging.getLogger().setLevel(logging.INFO)
        args = vars(parser.parse_args())
    
        if args['input'] != 'local':
            if not args['bucket'] or not args['project'] or not args['region']:
                print("Project, Bucket, Region are needed in order to run on the cloud on full dataset.")
                parser.print_help()
                parser.exit()
    
        run(project=args['project'], bucket=args['bucket'], region=args['region'], input=args['input'])
    ```


   Verify the results:
   ```
   cat /tmp/all_data*
   ```
* Run a Dataflow pipeline to create the full training dataset:
  ```
    python3 create_traindata.py --input bigquery --project <PROJECT> --bucket <BUCKET> --region <REGION>
  ```
  Note if you get an error similar to:
  ```
  AttributeError: Can't get attribute '_create_code' on <module 'dill._dill' from '/usr/local/lib/python3.7/site-packages/dill/_dill.py'>
  ```
  it is because the global version of your modules are ahead/behind of what Apache Beam on the server requires. Make sure to submit Apache Beam code to Dataflow from a pristine virtual environment that has only the modules you need:
  ```bash
  python -m venv ~/beamenv
  source ~/beamenv/bin/activate
  pip install apache-beam[gcp] google-cloud-aiplatform cloudml-hypertune pyfarmhash pyparsing==2.4.2
  python3 create_traindata.py ...
  ```
  Note that beamenv is only for submitting to Dataflow. Run train_on_vertexai.py and other code directly in the terminal.
* Run script that copies over the Ch10 model.py and train_on_vertexai.py files and makes the necessary changes:
  ```
  python3 change_ch10_files.py
  ```
* [Optional] Train an AutoML model on the enriched dataset:
  ```
  python3 train_on_vertexai_11.py --automl --project <PROJECT> --bucket <BUCKET> --region <REGION>
  ```
  Verify performance by running the following BigQuery query:
  ```sql
  SELECT  
  SQRT(SUM(
      (CAST(ontime AS FLOAT64) - predicted_ontime.scores[OFFSET(0)])*
      (CAST(ontime AS FLOAT64) - predicted_ontime.scores[OFFSET(0)])
      )/COUNT(*))
  FROM dsongcp.ch11_automl_evaluated
  ```
* Train custom ML model on the enriched dataset:
  ```
  python3 train_on_vertexai_11.py --project <PROJECT> --bucket <BUCKET> --region <REGION>
  ```
  Look at the logs of the log to determine the final RMSE.
* Run a local pipeline to invoke predictions:
    ```
    python3 make_predictions.py --input local
    ```
   Verify the results:
   ```
   cat /tmp/predictions*
   ```
* [Optional] Run a pipeline on full BigQuery dataset to invoke predictions:
    ```
    python3 make_predictions.py --input bigquery --project <PROJECT> --bucket <BUCKET> --region <REGION>
    ```
   Verify the results
   ```
   gsutil cat gs://BUCKET/flights/ch11/predictions* | head -5
   ```
* [Optional] Simulate real-time pipeline and check to see if predictions are being made

  
   In one terminal, type:
    ```
  cd ../04_streaming/simulate
  python3 ./simulate.py --startTime '2015-05-01 00:00:00 UTC' \
           --endTime '2015-05-04 00:00:00 UTC' --speedFactor=30 --project <PROJECT>
    ```
   
  In another terminal type:
    ```
    python3 make_predictions.py --input pubsub \
           --project <PROJECT> --bucket <BUCKET> --region <REGION>
    ```
  
  Ensure that the pipeline starts, check that output elements are starting to be written out, do:
   ```
   gsutil ls gs://BUCKET/flights/ch11/predictions*
   ```
   Make sure to go to the GCP Console and stop the Dataflow pipeline.

  
* Simulate real-time pipeline and try out different jagger etc.

  In one terminal, type:
    ```
  cd ../04_streaming/simulate
  python3 ./simulate.py --startTime '2015-02-01 00:00:00 UTC' \
           --endTime '2015-02-03 00:00:00 UTC' --speedFactor=30 --project <PROJECT>
    ```
   
  In another terminal type:
    ```
    python3 make_predictions.py --input pubsub --output bigquery \
           --project <PROJECT> --bucket <BUCKET> --region <REGION>
    ```
  
  Ensure that the pipeline starts, look at BigQuery:
   ```sql
   SELECT * FROM dsongcp.streaming_preds ORDER BY event_time DESC LIMIT 10
   ```
   When done, make sure to go to the GCP Console and stop the Dataflow pipeline.
   
   Note: If you are going to try it a second time around, delete the BigQuery sink, or simulate with a different time range
   ```
   bq rm -f dsongcp.streaming_preds
   ```
  
