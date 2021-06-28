try:

    from datetime import timedelta
    from datetime import datetime,date
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.email import EmailOperator
    import json
    import pandas as pd
    import os
    import requests
    from airflow.decorators import dag, task
    from airflow.utils.dates import days_ago
    from airflow.models import Variable
    import requests
    from airflow.hooks.postgres_hook import PostgresHook
    import time
    from airflow.utils.email import send_email
    

except Exception as e:
    print("Error  {} ".format(e))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 6, 1),
    'email': ['zhangtuomingz@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    "catchup": False,
}


dag = DAG(
    dag_id='Best Seller Book list',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['Best Seller Book list'],
)
API_KEY = 'HJX1K60DOIoWcZHacyntVKwVpKfyTF3f'


def extract_data():

    #Set default values for recipients
    recipients = ['zhangtuomingz@gmail.com']
    current = date.today() + timedelta(days=6)

    fiction_url = f"https://api.nytimes.com/svc/books/v3/lists/current/combined-print-and-e-book-fiction.json?api-key={API_KEY}"
    Non_fiction_url = f"https://api.nytimes.com/svc/books/v3/lists/current/combined-print-and-e-book-nonfiction.json?api-key={API_KEY}"

    requestHeaders = {
    "Accept": "application/json"
    }

    r1 = requests.get(fiction_url, headers=requestHeaders)
    fiction = r1.json()
    r2 = requests.get(Non_fiction_url, headers=requestHeaders)
    Non_fiction = r2.json()

    #If the data is successfully fetched and it will send out the emails with data
    if fiction['status'] == 'OK'& Non_fiction['status'] == 'OK':
        fiction_dict = {}
        Non_fiction_dict = {}

        def create_dataframe(books):
            rank = []
            book_title = []
            book_author = []


            for record in books["results"]["book"]:
                rank.append(record['rank'])
                book_title.append(record['title'])
                book_author.append(record['author'])


            book_dict = {
                'rank': rank,
                'book_title': book_title,
                'book_author':book_author,

            }
            temp_df = pd.DataFrame(data=book_dict,columns=['rank','book_title','book_author'])
            return temp_df 

        #Create dataframe for fiction book list and nonfiction book list
        fiction_book_list_df = create_dataframe(fiction)
        Nonfiction_book_list_df = create_dataframe(Non_fiction)

        #Perpare the data for email sending
        with open('fiction_book_list.txt', 'w') as the_file:
            the_file.write(
                fiction_book_list_df.to_string(header = False, index = False)
            )
        with open('Nonfiction_book_list.txt', 'w') as the_file:
            the_file.write(
                Nonfiction_book_list_df.to_string(header = False, index = False)
            )
        send_email('zhangtuomingz@gmail.com','books',html_content= 'templates/email.html',files=[os.getcwd() + "/Fiction_book_list.txt",
        os.getcwd() + "/Nonfiction_book_list.txt"])

#perform PythonOperator
extract_and_send = PythonOperator(
    task_id='best_sellers_book_list',
    python_callable=extract_data,
    dag=dag,   
)

#email = EmailOperator(
 #               task_id='send_email',
  #              to= ['zhangtuomingz@gmail.com'],
   #             subject='NYTimes Best Seller Book List',
    #            html_content= 'templates/email.html',
     #       )


extract_and_send