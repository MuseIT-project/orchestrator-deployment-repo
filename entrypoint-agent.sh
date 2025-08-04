#!/usr/bin/env sh

<<<<<<< Updated upstream
prefect config set PREFECT_API_URL="http://0.0.0.0:4200/api"
sleep 2 
=======
prefect config set PREFECT_API_URL="http://prefect-museit:4200/api"
sleep 8 
>>>>>>> Stashed changes
prefect worker start --pool default
