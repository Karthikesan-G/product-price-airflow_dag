import pandas as pd
import mysql.connector
from datetime import datetime
import random

def insert_data(host, user, password, database):
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    cursor = conn.cursor()

    df = pd.read_csv("Output.csv")

    for _, row in df.iterrows():
        try:
            query = """
                insert into books (
                    Title, Date_Added, Descriptions, Category_Name, Star_Rating, Price, Tax, Price_with_Tax,
                    Availability, Availability_Count, UPC, Product_Type, Number_of_Reviews,
                    Image_URL, Book_Link
                ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                row['Title'],
                '2024-06-30',
                row['Description'],
                row['Category Name'],
                float(row['Star Rating']),
                float(row['Price']) + random.randint(-10, 30),
                float(row['Tax']),
                float(row['Price with Tax']),
                row['Availability'],
                int(row['Availability Count']),
                row['UPC'],
                row['Product Type'],
                int(row['Number of Reviews']),
                row['Image URL'],
                row['Book Link']
            )

            cursor.execute(query, values)
            conn.commit()

            # input()
        except Exception as e:
            print("error : ", e)

    cursor.close()
    conn.close()

def check_price_diff(host, user, password, database):

    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    cursor = conn.cursor()

    query = """
        select 
            b1.id,
            b1.Title,
            (b2.Price - b1.Price) as Price_Diff
        from books b1
        join books b2
        on b1.Date_Added = DATE_sub(b2.Date_Added, interval 1 day) 
        and b1.UPC = b2.UPC
        where (b2.Price - b1.Price) > 10
    """
    
    cursor.execute(query)
    results = cursor.fetchall()

    for i in range(10):
        print(results[i])
    # df = pd.read_sql_query(query, conn)
    # print(df.head(10))

    cursor.close()
    conn.close()
