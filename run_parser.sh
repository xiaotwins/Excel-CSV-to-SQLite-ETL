#!/bin/bash

## import environment variables ##

export SPARK_MAJOR_VERSION=2

now=$(date +"%m_%d_%Y")
day_minus_seven=$(date +"%m_%d_%Y" -d "7 day ago")

cd /root/path/to/data

FILE_COUNT=$(ls | wc -l)
total_file_count=$FILE_COUNT
initial_file_count=0
if [ $total_file_count -gt $initial_file_count ]
then
   cd /root/path/to/etl_parser/
   source /root/anaconda2/bin/activate && python ./main.py ./all_data ./transformation.json ./all_data ./file_mapping.json &>> ./parser_logs/etl_parser_log_$now.txt
else
   echo "No File Found"
fi

rm -f ./parser_logs/etl_parser_log_$day_minus_seven.txt


