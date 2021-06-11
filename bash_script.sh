#!/bin/bash
REGION=us-central1
ZONE=us-central1-c
PROJECT_ID=week-2-de-blank-space-johan
CLUSTER_NAME=dataproc-johan-week3
BUCKET_NAME=week-3-johan
echo "STARTING THE SCRIPT"
echo "MAKE OUTPUT FOLDER FOR TRANSFORM_DATA.PY"
mkdir data_output
echo "CREATING FOLDER DONE"
gcloud config set project ${PROJECT_ID}
echo "RUN THE PYTHON SCRIPT TO DO SIMPLE TRANSFORMATION"
python transform_data.py
echo "TRANSFORM DATA DONE"
# gsutil -m cp -r D:/DS/Academi/Week_3/Johan/data_output gs://week-3-johan/data
echo "LOAD TO GCS SUCCESS"
echo "ENABLE DATAPROC APIS"
gcloud services enable compute.googleapis.com \
    dataproc.googleapis.com \
    bigquerystorage.googleapis.com
echo "CREATE CLUSTER DATAPROC"
gcloud beta dataproc clusters create ${CLUSTER_NAME} \
  --region=${REGION} \
  --zone=${ZONE} \
  --single-node \
  --master-machine-type=n1-standard-2 \
  --bucket=${BUCKET_NAME} \
  --image-version=1.5-ubuntu18 \
  --optional-components=ANACONDA,JUPYTER \
  --enable-component-gateway \
  --metadata 'PIP_PACKAGES=google-cloud-bigquery google-cloud-storage' \
  --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/python/pip-install.sh
echo "CREATING CLUSTER DONE"
echo "SUBMIT SPARKJOB"
gcloud dataproc jobs submit pyspark \
    gs://week-3-johan/sparkjob/sparkjob1.py \
  --cluster=${CLUSTER_NAME} \
  --region=${REGION} \
  --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
echo "SUBMITING SPARKJOB DONE"
echo "NOW DELETING THE CLUSTER"
gcloud dataproc clusters delete dataproc-johan-week3 --region=us-central1
echo "DELETING CLUSTER DONE"
echo "END OF THE SCRIPT"