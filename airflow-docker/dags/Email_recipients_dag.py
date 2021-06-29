try:
    from airflow import DAG
    from airflow.operators.email import EmailOperator
    from airflow.operators.python import PythonOperator
    from datetime import timedelta, datetime
    from airflow.utils.dates import days_ago
    from airflow.sensors.external_task_sensor import ExternalTaskSensor
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    from airflow.utils.email import send_email


except Exception as e:
    print("Error  {} ".format(e))


#set up the default arguments
default_args = {
    'owner': '',
    'depends_on_past': True,
    'start_date': datetime(2021, 6, 25),
    'email': ['zhangtuomingz@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    "catchup": True,
}

with DAG( 
    dag_id='Email_recipients_list',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(0),
    default_args = default_args,
    catchup=False,
    tags=['Email Recipients']
    ) as dag:

    #monitor etl pipelines
    sensor=ExternalTaskSensor(
        task_id='sensor',
        external_dag_id='Best_sellers_book_list_ETL',
        external_task_id='load_Book_list'
    )

    #send out the email
    email = EmailOperator(
                task_id='send_email',
                to= ['zhangtuomingz@gmail.com', "{{dag_run.conf['email']}}"],
                subject='NYTimes Best Seller Book List',
                html_content= 'templates/email.html',
            )

    
    
    sensor >> email