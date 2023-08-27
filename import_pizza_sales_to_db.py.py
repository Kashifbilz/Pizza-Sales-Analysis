#!/usr/bin/env python
# coding: utf-8

# In[2]:


#Importing data from a CSV file into a PostgreSQL database

import csv
import psycopg2
from datetime import datetime

# PostgreSQL database connection parameters
db_params = {
    'dbname': 'pizza_db',
    'user': 'postgres',
    'password': 'admin',
    'host': 'localhost',
    'port': '5432'
}

# CSV file path
csv_file_path = r"C:\Users\Public\Downloads\pizza_sales.csv"

# Connect to the PostgreSQL database
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Read CSV and insert data into the PostgreSQL table
with open(csv_file_path, 'r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        # Convert date format from "DD-MM-YYYY" to "YYYY-MM-DD"
        order_date = datetime.strptime(row['order_date'], '%d-%m-%Y').date()
        
        # Build and execute the INSERT statement
        insert_query = f"""
            INSERT INTO pizza_sales (pizza_id, order_id, pizza_name_id, quantity, order_date, order_time, unit_price, total_price, pizza_size, pizza_category, pizza_ingredients, pizza_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        data_tuple = (
            int(row['pizza_id']),
            int(row['order_id']),
            row['pizza_name_id'],
            int(row['quantity']),
            order_date,
            row['order_time'],
            float(row['unit_price']),
            float(row['total_price']),
            row['pizza_size'],
            row['pizza_category'],
            row['pizza_ingredients'],
            row['pizza_name']
        )
        cursor.execute(insert_query, data_tuple)

# Commit changes and close the database connection
conn.commit()
cursor.close()
conn.close()

print("Data imported successfully.")


# In[ ]:




