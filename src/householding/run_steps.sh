#! /bin/bash


### BELOW ARE THE STEPS TO RUN THE PIPELINE. 

### 1. Clean evrything up
#delete the db
rm ../cperf.duckdb
#clean dbt via cli
poetry run dbt clean

### 2. Seed indput data
./seeds/csv_import.sh
# a quick check that tables indeed exist
duckdb ../cperf.duckdb "SHOW ALL TABLES;"


###  3. Run the dbt 'pipeline' ( models, tests, snapshots, etc) 
poetry run dbt deps
poetry run dbt build


### 4. Export results
#export result into txt file
duckdb ../cperf.duckdb "COPY (select * from hh.cperf) TO '../cperf.txt' (HEADER, DELIMITER ',');" 
