#!/usr/bin/env sh

prefect config set PREFECT_API_URL="http://0.0.0.0:4200/api"
sleep 2 
prefect worker start --pool default
