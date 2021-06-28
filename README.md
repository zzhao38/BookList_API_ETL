# BookList_API_ETL

The project is create to extract, transform and load the best seller book list using NyTimes Book API. 

Base on the requirement, firstly, the DAG runs daily and needs to only perform extracting and sending once a week. Secondly, a email recipients list need to be create to receive notification once the best seller list is updated. 

##Start Airflow

To start with Docker Compose with the following command:

```bash
docker-compose up -d
```

Airflow webserver at http://localhost:8080.


Email-Sending Can be triggered manually 