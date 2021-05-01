#!/bin/bash
mkdir -p zipped
zip -r zipped/$1 results/$1/ logs/$1/ remote_logs/$1/ meta_results/$1/ -qi '*.data' '*.out' '*.error' '*.zip'
echo "Results, logs and meta results of $1 compressed into zipped/$1.zip"