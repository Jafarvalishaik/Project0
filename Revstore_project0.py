#!/usr/bin/env python
# coding: utf-8

# # 1. Data Reading:

# In[5]:


import pandas as pd
import mysql.connector
from mysql.connector import Error
import json
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
# Function to load JSON data into a DataFrame
def load_json_to_df(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return pd.DataFrame(data)

# Load customer data
customer_df = load_json_to_df('customers.json')

# Load transaction data
transaction_df = load_json_to_df('transaction_logs.json')
print(customer_df)
print(transaction_df)


# # 2. Exploratory Data Analysis (EDA):

# # a)Understand Data Structure

# In[3]:


# Display the first few rows of each DataFrame
print("Customer Data:")
print(customer_df.head())

print("\nTransaction Data:")
print(transaction_df.head())

# Display DataFrame information
print("\nCustomer Data Info:")
print(customer_df.info())

print("\nTransaction Data Info:")
print(transaction_df.info())


# # b) Identify Key Data Attributes

# In[4]:


#. Identify Key Data Attributes
# Display summary statistics for numerical attributes
print("\nCustomer Data Description:")
print(customer_df.describe(include='all'))

print("\nTransaction Data Description:")
print(transaction_df.describe(include='all'))


# # c)Detect Missing Values

# In[5]:


# Check for missing values in customer data
print("\nCustomer Data Missing Values:")
print(customer_df.isnull().sum())

# Check for missing values in transaction data
print("\nTransaction Data Missing Values:")
print(transaction_df.isnull().sum())


# # d)Detect Outliers

# In[7]:


# Function to detect outliers based on IQR
def detect_outliers(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    return df[(df[column] < (Q1 - 1.5 * IQR)) | (df[column] > (Q3 + 1.5 * IQR))]

# Detect outliers in transaction amounts
outliers = detect_outliers(transaction_df, 'price')
print("\nTransaction Data Outliers:")
print(outliers)


# # e)Visualize Data

# In[13]:



# Visualize distribution of transaction amounts
plt.figure(figsize=(10, 6))
sns.histplot(transaction_df['product_category'], bins=30, kde=True)
plt.title('Distribution of Transaction Amounts')
plt.xlabel('price')
plt.ylabel('Frequency')
plt.show()

# Visualize missing values in customer data
plt.figure(figsize=(10, 6))
sns.heatmap(customer_df.isnull(), cbar=False, cmap='viridis')
plt.title('Missing Values Heatmap for Customer Data')
plt.show()


# # 3.Database Schema Design:

# # Customer
# ---------------------------
# customer_id (PK)
# customer_name
# country
# city
# 
# Product
# ---------------------------
# product_id (PK)
# product_name
# product_category
# 
# Order
# ---------------------------
# order_id (PK)
# customer_id (FK)
# datetime
# ecommerce_website_name
# 
# OrderItem
# ---------------------------
# order_id (FK)
# product_id (FK)
# qty
# price
# 
# Payment
# ---------------------------
# payment_txn_id (PK)
# order_id (FK)
# payment_type
# payment_txn_success
# failure_reason
# 
# Relationships:
# - Customer (customer_id) → Order (customer_id)
# - Order (order_id) → OrderItem (order_id)
# - Product (product_id) → OrderItem (product_id)
# - Order (order_id) → Payment (order_id)
# 

# # 4.Data Mapping and Transformation:
# 

# In[17]:


#Ensure correct data types- Data transformers
#customer_dataframe
customer_df['customer_id'] = customer_df['customer_id'].astype(int)
customer_df['customer_name'] =customer_df['customer_name'].astype(str)
customer_df['country'] = customer_df['country'].astype(str)
customer_df['city'] = customer_df['city'].astype(str)
print(customer_df.dtypes)
#transaction_logs dataframe
# Convert columns to appropriate data types
transaction_df = transaction_df.astype({
    "order_id": 'int64',  # Integer
    "customer_id": 'int64',  # Integer
    "product_id": 'int64',  # Integer
    "product_name": 'str',  # String
    "product_category": 'str',  # String
    "payment_type": 'str',  # String
    "qty": 'int64',  # Integer
    "price": 'float64',  # Float
    "datetime": 'datetime64[ns]',  # DateTime
    "ecommerce_website_name": 'str',  # String
    "payment_txn_id": 'str',  # String
    "payment_txn_success": 'bool',  # Boolean
    "failure_reason": 'str'  # String (can also be 'object' if mixed types)
})

# Print the DataFrame to verify
print(transaction_df.dtypes)


# In[1]:





# # Data Loading

# In[6]:


config = {
    'user': 'root',
    'password': 'Jafar@2024',
    'host': 'localhost',
    'database': 'Revproject0'
}
def load_data_to_mysql(df, table_name, columns):
    """Load data from DataFrame to MySQL table."""
    try:
        # Establish the connection
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()

        # Prepare the SQL insert statement
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        # Convert DataFrame to list of tuples
        data = df[columns].values.tolist()

        # Execute the insert statement for all rows
        cursor.executemany(sql, data)

        # Commit the transaction
        connection.commit()
        print(f"Data loaded into {table_name} successfully!")

    except Error as e:
        print(f"Error: {e}")

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
# Extract Product DataFrame

def main():
    # customer DataFrame
    df_cust=customer_df[['customer_id','customer_name','country','city']].drop_duplicates().reset_index(drop=True)
    df_product = transaction_df[['product_id', 'product_name', 'product_category']].drop_duplicates().reset_index(drop=True)
    # Product DataFrame
    # Order DataFrame
    df_order = transaction_df[['order_id', 'customer_id', 'datetime', 'ecommerce_website_name']].drop_duplicates().reset_index(drop=True)
     # OrderItem DataFrame
    df_orderitem = transaction_df[['order_id', 'product_id', 'qty', 'price']].reset_index(drop=True)
     # Payment DataFrame
    df_payment = transaction_df[['order_id', 'payment_type', 'payment_txn_id', 'payment_txn_success', 'failure_reason']].drop_duplicates().reset_index(drop=True)
    # Define columns for each table
    customer_columns = ['customer_id', 'customer_name', 'country', 'city']
    product_columns = ['product_id', 'product_name', 'product_category']
    order_columns = ['order_id', 'customer_id', 'datetime', 'ecommerce_website_name']
    order_item_columns = ['order_id', 'product_id', 'qty', 'price']
    payment_columns = ['payment_txn_id', 'order_id', 'payment_type', 'payment_txn_success', 'failure_reason']

    # Prepare separate DataFrames for each table
    customer_df1 = df_cust[customer_columns].drop_duplicates(subset=['customer_id'])
    product_df = df_product[product_columns].drop_duplicates(subset=['product_id'])
    order_df = df_order[order_columns].drop_duplicates(subset=['order_id'])
    order_item_df = df_orderitem[order_item_columns]
    payment_df = df_payment[payment_columns]

    # Load data into MySQL tables
    load_data_to_mysql(customer_df1, 'Customer', customer_columns)
    load_data_to_mysql(product_df, 'Product', product_columns)
    load_data_to_mysql(order_df, 'Order', order_columns)
    load_data_to_mysql(order_item_df, 'OrderItem', order_item_columns)
    load_data_to_mysql(payment_df, 'Payment', payment_columns)

if __name__ == "__main__":
    main()


# In[2]:


import mymodulesql


# In[1]:





# In[ ]:




