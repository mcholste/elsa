#!/bin/sh
# This script will upload the file given as the first argument to the server configured below.

# If on Mac OSX, change "md5sum" below to "md5 -q"

# Set URL/USER/APIKEY to be a url/user/apikey combination found in the target's apikeys config
ELSA_SERVER_URL="https://127.0.0.1/API/upload"
USER="elsa"
APIKEY="myAPIkey-from-elsa_web.conf"

EPOCH=$(date '+%s');
HASH=$(printf '%s' "$EPOCH$APIKEY" | shasum -a 512);
HEADER=$(echo "Authorization: ApiKey $USER:$EPOCH:$HASH" | sed -e s'/ \-//')
FILENAME=$1
NAME=$FILENAME
MD5=$(md5sum $FILENAME | cut -f1 -d\ | tr -d "\n")
curl -XPOST -H "$HEADER" -F "name=$NAME" -F "start=$EPOCH" -F "end=$EPOCH" -F "md5=$MD5" -F "filename=@$FILENAME;filename=$FILENAME" --insecure $ELSA_SERVER_URL
