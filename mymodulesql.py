#!/usr/bin/env python
# coding: utf-8

# In[1]:


import mysql.connector
from mysql.connector import Error

# MySQL connection parameters
config = {
    'user': 'root',
    'password': 'Jafar@2024',
    'host': 'localhost',
    'database': 'Revproject0'
}

def create_schemas():
    try:
        # Establish the connection
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()

        # Define schema creation SQL statements
        create_customers_table = """
        CREATE TABLE IF NOT EXISTS Customer (
            customer_id INT AUTO_INCREMENT PRIMARY KEY,
            customer_name VARCHAR(255) NOT NULL,
            country VARCHAR(255),
            city VARCHAR(255)
        );
        """

        create_products_table = """
        CREATE TABLE IF NOT EXISTS Product (
            product_id INT AUTO_INCREMENT PRIMARY KEY,
            product_name VARCHAR(255) NOT NULL,
            product_category VARCHAR(255)
        );
        """

        create_orders_table = """
        CREATE TABLE IF NOT EXISTS `Order` (
            order_id INT AUTO_INCREMENT PRIMARY KEY,
            customer_id INT,
            datetime DATETIME NOT NULL,
            ecommerce_website_name VARCHAR(255),
            FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
        );
        """

        create_order_items_table = """
        CREATE TABLE IF NOT EXISTS OrderItem (
            order_id INT,
            product_id INT,
            qty INT NOT NULL,
            price DECIMAL(10, 2) NOT NULL,
            PRIMARY KEY (order_id, product_id),
            FOREIGN KEY (order_id) REFERENCES `Order`(order_id),
            FOREIGN KEY (product_id) REFERENCES Product(product_id)
        );
        """

        create_payments_table = """
        CREATE TABLE IF NOT EXISTS Payment (
            payment_txn_id VARCHAR(255) PRIMARY KEY,
            order_id INT,
            payment_type VARCHAR(255) NOT NULL,
            payment_txn_success VARCHAR(50) NOT NULL,
            failure_reason VARCHAR(255),
            FOREIGN KEY (order_id) REFERENCES `Order`(order_id)
        );
        """

        # Execute the SQL commands to create the tables
        cursor.execute(create_customers_table)
        cursor.execute(create_products_table)
        cursor.execute(create_orders_table)
        cursor.execute(create_order_items_table)
        cursor.execute(create_payments_table)

        # Commit the transaction
        connection.commit()
        print("Schema created successfully!")

    except Error as e:
        print(f"Error: {e}")
    
# This line will only execute if this script is run directly
if __name__ == "__main__":
    create_schemas()


# In[ ]:




