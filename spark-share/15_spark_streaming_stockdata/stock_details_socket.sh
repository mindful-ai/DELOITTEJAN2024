#!/bin/bash

while IFS= read line
do
        # display $line or do somthing with $line
        echo "$line"
        sleep 0.5s
done <"$1"

