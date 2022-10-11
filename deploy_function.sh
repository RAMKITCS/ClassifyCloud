#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SOURCE_DIR="${DIR}"

gcloud functions deploy classification-trigger \
--gen2 \
--runtime=python38 \
--region=us-central1 \
--source=${SOURCE_DIR} \
--entry-point=hello_gcs \
--trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
--trigger-event-filters="bucket=noted-cortex-358103_research3" \
--trigger-location="us" \
--memory=2048 \
--timeout=540 \
--max-instances=100 \
--env-vars-file .env.yaml