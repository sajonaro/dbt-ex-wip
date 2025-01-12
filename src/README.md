
## 101

- to run the project
    ```bash
    # run the main script
    $ cd scr/householding/
    $ ./run_steps.sh
    ``` 

- prerequisite: install DuckDB (on windows)
    ```bash
    winget install DuckDB.cli
    ```


### useful scrips

- create symbolic (soft) links to data files
    ```bash
    #create symlinks to input data
    ln -s /e/Projects/Monterey-01/Cperf.txt cperf_sample_output.morph
    ln -s /e/Projects/Monterey-01/CustData.morph CustData.morph
    ln -s ~/.dbt/profiles.yml profiles.yml
    ln -s /e/Projects/Monterey-01/AMD0103000224020100029315.txt AMD0103000224020100029315.txt
    ln -s /e/Projects/Monterey-01/AMI0103000324020100029315.txt AMI0103000324020100029315.txt

    ```