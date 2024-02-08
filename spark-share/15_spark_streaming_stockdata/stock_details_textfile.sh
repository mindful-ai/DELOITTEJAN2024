#!/bin/bash

if [ -e /tmp/streamdir ]; then
  rm -r /tmp/streamdir
fi
mkdir /tmp/streamdir

i=$2
while IFS= read line
do
    # echo $line into a file
    i=$(expr $i + 1)
    echo "$line" > /tmp/streamdir/file_$i
    sleep 0.5s
done <"$1"

