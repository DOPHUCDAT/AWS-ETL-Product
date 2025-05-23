from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

#fetch data 
header = {
    "Referer" : "https://www.amazon.com/ref=nav_logo",
    "Sec-Ch-Ua": "Microsoft Edge",
    "Sec-Ch-Ua-Platform": "Windows",
    "User-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"
}
#Extract and Transform
def get_item(num, ti):
    base_url = f"https://www.amazon.com/s?k=elden+ring&crid=3HAM01VJPAK40&sprefix=%2Caps%2C367&ref=nb_sb_ss_recent_1_0_recent"
    items = []
    #Lưu lại tiêu đề đã xem vào trong kiểu set để tránh duplicate
    seen_title = set()  
    page = 1
    session = requests.Session()
    session.headers.update(header)
    while len(items) < num:
        url = f"{base_url}&page={page}"
        response = session.get(url)
        #Mã check xem yêu cầu có thành công không (200 là ok, 404 not found)
        if response.status_code == 200:  
            soup = BeautifulSoup(response.content, "html.parser")
            item_containers = soup.find_all("div",{"class": "sg-col-inner"})

            for item in item_containers:
                title = item.find("a",{"class" : "a-link-normal s-line-clamp-4 s-link-style a-text-normal"})
                price = item.find("span",{"class" : "a-price-whole"})
                rating = item.find("span",{"class" : "a-icon-alt"})
                if title and price and rating:
                    item_title = title.text.strip()
                    if item_title not in seen_title:
                        seen_title.add(item_title)
                        items.append({
                            "Title": item_title,
                            "Price": price.text.strip(),
                            "Rating": rating.text.strip()
                        })
            #Sang trang tiep theo de lap lai 
            page += 1
        else:
            print("Failed to retrieve the page")
            break
    #Gioi han lai so luong san pham theo yeu cau
    items = items[:num]
    df = pd.DataFrame(items)
    #Lam sach du lieu
    df.drop_duplicates(subset="Title", inplace=True)
    #Day df vao XCom (truyen du lieu)
    ti.xcom_push(key='item_data', value=df.to_dict('records'))

#Load
def Load_into_postgres(ti):
    item_data = ti.xcom_pull(key='item_data', task_ids='extract_data')
    if not item_data:
        raise ValueError("No item data found")
    postgres_hook = PostgresHook(postgres_conn_id='items_connection')
    insert_query = """
    INSERT INTO items(title, price, rating)
    VALUES(%s, %s, %s)
    """

    for item in item_data:
        postgres_hook.run(insert_query, parameters=(item['Title'], item['Price'], item['Rating']))


#DAG and Tasks
default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2024,2,18),
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

dag = DAG(
    'ETL_pipeline',
    default_args=default_args,
    description='A simple DAG to extract item data from Amazon and store it in Postgres',
    schedule_interval='@daily',
    catchup= False
)   

extract_data_task = PythonOperator(
    task_id = 'extract_data',
    python_callable=get_item,
    op_args=[30],
    dag=dag
)

create_table_task = PostgresOperator(
    task_id ='create_table',
    postgres_conn_id='items_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS items (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        price TEXT,
        rating TEXT
    );
    """,
    dag=dag
)

insert_data_task = PythonOperator(
    task_id ='insert_data',
    python_callable=Load_into_postgres,
    dag=dag
)

extract_data_task >> create_table_task >> insert_data_task
 
