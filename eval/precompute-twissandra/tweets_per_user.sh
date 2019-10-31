#!/bin/bash

# NOTE: We assume a "cleaned" tweets file!
# Format: username;uuid;tweet

file=${1}
time cat ${file} | cut -f 1 -d";" | sort | uniq -c > users_${file}

