# Data Extraction from MyTransport API
This is a data engineering project that uses Airflow to automate the extraction of bus arrival data from the MyTransport API.

# Project Structure
de_sg_transport
├── dags
│   └── dag_bus_info.py
└── src
    ├── __init__.py
    └── extraction.py

dags/*.py: This folder contains the definitions of the Airflow DAG that orchestrates the ETL process.
src/*.py: This folder contains the application code for the ETL process.

# Requirements
Python 3.8 or newer
Docker and Docker Compose
An account key and authorization token from LTA MyTransport API
Create a new file .env in the project directory and define the following environment variables:
`ACCOUNT_KEY=YourMyTransportAPIAccountKey`

# Instructions
Start the Docker services, including Airflow, with Docker Compose:
`docker-compose up`

You may want to run this command in the background by adding -d:
`docker-compose up -d`

Wait for service to be up, then access the Airflow UI at http://localhost:8080.

Login with username `airflow` and password `airflow`

Enable the DAG with the name 'lta_api_dag'.

The DAG is scheduled to run once a day. You can also manually trigger a run from the Airflow UI.

After a DAG run, the data will be saved in the data directory in the project directory. The filename will reflect the timestamp of the extraction.

# Development
To add or modify the ETL process, you can edit dag/ and src/ as needed. You will need to restart the Docker services for your changes to take effect:

When done, 
`docker-compose down`

