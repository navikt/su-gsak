#!/usr/bin/env bash

DIR=/var/run/secrets/nais.io/srvuser

echo "Attempting to export serviceuser from $DIR if it exists"

if test -d $DIR;
then
    for FILE in `ls $DIR`
    do
       echo "- exporting $FILE"
       export $FILE=`cat $DIR/$FILE`
    done
fi
