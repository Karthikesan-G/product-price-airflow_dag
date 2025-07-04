import requests
import os
from bs4 import BeautifulSoup
import re
import pandas as pd
import logging
import ast
import mysql.connector
import random

def fetch_products(**kwargs):

    try:
        req = requests.Session()

        url = 'https://fakestoreapi.com/products'

        content = req.get(url)
        logging.info(content.status_code)

        json_parsed_content = content.json()
        # logging.info(json_parsed_content)

        df = pd.DataFrame(json_parsed_content)

        ti = kwargs['ti']
        path = ti.xcom_pull(key='path')

        df.to_csv(f'{path}raw_output.csv', header=True, index=False)
        logging.info(os.getcwd())

    except Exception as e:
        logging.info(e)



def clean_products(**kwargs):

    ti = kwargs['ti']
    path = ti.xcom_pull(key='path')

    df = pd.read_csv(f'{path}raw_output.csv')

    df['rating'] = df['rating'].apply(ast.literal_eval)
    df['rating_count'] = df['rating'].apply(lambda x: x['count'])
    df['rating'] = df['rating'].apply(lambda x: x['rate'])
    df['price'] = random.randint(-10, 30) + df['price']

    df.columns = ['product_id', 'product_name', 'price','description', 'category', 'image_link', 'rating', 'rating_count']
    
    df.to_csv(f'{path}raw_output.csv', header=True, index=False)

def load_data(host, user, password, database, **kwargs):

    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = database,
    )

    cursor = conn.cursor()
    
    ti = kwargs['ti']
    path = ti.xcom_pull(key='path')

    df = pd.read_csv(f'{path}raw_output.csv')

    for _, row in df.iterrows():
        
        product_id = int(row['product_id'])
        product_name = row['product_name']
        price = round(float(row['price']), 2)
        category = row['category']
        description = row['description']
        image_link = row['image_link']
        rating = round(float(row['rating']), 2)
        rating_count = round(float(row['rating_count']), 2)

        inserT = """
            INSERT INTO products_table (
            product_id, product_name, price, category, description, image_link, rating, rating_count
            ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
            )

        """

        values = (product_id, product_name, price, category, description, image_link, rating, rating_count)

        cursor.execute(inserT, values)
        conn.commit()
    
    cursor.close()
    conn.close()
