#!/bin/bash

echo SECRET_GCP_SERVICE_ACCOUNT=$(cat /home/andres/.gcp/fruit-packing-plant-simulator-e17707a8f961.json | base64 -w 0) > .env_encoded # enter path to service account key
echo SECRET_EMAIL_PASSWORD=$(cat ./.email_password | base64 -w 0) >> .env_encoded # create text file with email password for alerts
