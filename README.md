# batch-streaming-postgreSQL

Create a postgres database and create a table to store the sample data from the **scanner_data.csv**

Run airflow webserver and airflow scheduler and after running it can be access by the **http://localhost:8080**

Make a connection string in the airflow's connection tab from its web UI

Configure your database connections in the **Filling_DB_by_rawdata.py** and run this code this will load the dummy data in the database

Run **bs_pg.py**, the dag will be submitted to the airflow and can be monitored through the airflow web UI

**bs_pg DAG** does the batch streaming on the interval of 2 minutes and dumps the data in the **output.csv** which is our datalake

**NOTE:** Make sure to make the amendments according to your enviroment variable and paths
