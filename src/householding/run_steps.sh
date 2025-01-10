#! /bin/bash

#convert txt to csv (and remove extra /t symbols)
# ./seeds/csv_clean.sh



#remove db
rm ../cperf.duckdb
poetry run dbt clean

#and import data into db
./seeds/csv_import.sh

#see tables
duckdb ../cperf.duckdb "SHOW ALL TABLES;"

#build
poetry run dbt deps
poetry run dbt build


#export result into txt file
duckdb ../cperf.duckdb "COPY (select * from hh.stg_ami) TO '../cperf.txt' (HEADER, DELIMITER ';');" 







