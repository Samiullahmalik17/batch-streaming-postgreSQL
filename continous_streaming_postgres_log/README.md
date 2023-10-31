# Continous-streaming-postgreSQL

Insert the data in postgres database

Run airflow webserver and airflow scheduler and after running it can be access by the **http://localhost:8080**

Make a connection string in the airflow's connection tab from its web UI

Configure your database connections in the **Filling_DB_by_rawdata.py** and run this code this will load the dummy data in the database

Run **log_continousstreaming.py**, the dag will be submitted to the airflow and can be monitored through the airflow web UI

**cs_pg DAG** does the streaming on the interval of 1 seconds and dumps the data in the **streaming_log.csv** which is our datalake and any modification that occur in database will be logged in datalake

**NOTE:** Make sure to make the amendments according to your enviroment variable and paths
