
try:

    from datetime import timedelta,date
    from airflow import DAG
    from datetime import datetime
    import json
    import pandas as pd
    import os
    from airflow.decorators import dag, task
    from airflow.utils.dates import days_ago
    from airflow.models import Variable
    import requests
    from airflow.hooks.postgres_hook import PostgresHook
    import time
    from sqlalchemy import create_engine
    from airflow.exceptions import AirflowFailException
    

except Exception as e:
    print("Error  {} ".format(e))

#set up the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2021, 6, 1),
    'email': ['zhangtuomingz@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    "catchup": False,
}

#set up the task will be run daily
schedule_interval = "@daily"

#upcoming_upadate_date = "2021-07-04"

#My_API_KEY = 'HJX1K60DOIoWcZHacyntVKwVpKfyTF3f'

dag = DAG('Book_list_etl', 
    default_args=default_args, 
    schedule_interval=schedule_interval, 
    start_date=days_ago(0),
    catchup=False,
    tags=['Book_list_ETL'])


def Best_sellers_book_list_ETL():
    

    def extract_Book_list(
        #Extracts best seller book list for
        #"combined-print-and-e-book-fiction" and "combined-print-and-e-book-nonfictionand"
        #returns book_list

        #set the defualt values for date, and defualt value for list 
        date: str = 'current', 
        list_name: list = ['combined-print-and-e-book-fiction','combined-print-and-e-book-nonfictionand']) -> dict :
        requestHeaders = {"Accept": "application/json"}

        date = Variable.get('upcoming_upadate_date')
        api_key = Variable.get('My_nyt_API_KEY')

        for list1 in list_name:
            requestUrl = f"https://api.nytimes.com/svc/books/v3/lists/{date}/{list1}.json?api-key={api_key}"
            data = requests.get(requestUrl, requestHeaders)
            if not data:
                raise AirflowFailException('Date is not valid or data is not available')
            else:    
                best_sellers_book_list.append(data.json()['results'])
        
    #Update the upcoming_upadate_date after the current request work
        upcoming_upadate_date = datetime.strptime(date, '%Y-%m-%d')
        upcoming_upadate_date += timedelta(days=7)
        upcoming_upadate_date = upcoming_upadate_date.strftime('%Y-%m-%d')
        Variable.set('upcoming_upadate_date', upcoming_upadate_date)

        return best_sellers_book_list

    def transform_Book_list(best_sellers_book_list: list) -> list :
        
    #using pandas DataFrame to handle the json data from Api    
    
        combined_print_and_e_book_fiction = pd.DataFrame(best_sellers_book_list[0]['books'], columns=['title','author','rank'])
        combined_print_and_e_book_nonfiction = pd.DataFrame(best_sellers_book_list[1]['books'], columns=['title','author','rank'])
        combined_print_and_e_book_fiction.rename(columns={'rank':'ranking'}, inplace=True)
        combined_print_and_e_book_nonfiction.rename(columns={'rank':'ranking'}, inplace=True)

        return [combined_print_and_e_book_fiction,combined_print_and_e_book_nonfiction]

    def load_Book_list(df_book_list: list):
    #save the data to target mysql database

        engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres/postgres")

        df_fiction = df_book_list[0]
        df_nonfiction = df_book_list[1]

        df_fiction.to_sql('bestsellers_fiction', con=engine, if_exists='replace')
        df_nonfiction.to_sql('bestsellers_nonfiction', con=engine, if_exists='replace')

    best_sellers_book_list_data = extract_Book_list()
    transform_Book_list_data = transform_Book_list(best_sellers_book_list_data)
    load_Book_list(transform_Book_list_data)

BookLists_api_etl = Best_sellers_book_list_ETL()

BookLists_api_etl