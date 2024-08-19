#!/usr/bin/env python
# coding: utf-8

# In[2]:


import mysql.connector
from mysql.connector import Error
import pandas as pd

# MySQL connection parameters
config = {
    'user': 'root',
    'password': 'Jafar@2024',
    'host': 'localhost',
    'database': 'Revproject0'
}

def fetch_data(query):
    """Fetch data from the database based on the query."""
    try:
        # Establish the connection
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()

        # Execute the query
        cursor.execute(query)
        # Fetch all rows
        result = cursor.fetchall()

        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
        return df

    except Error as e:
        print(f"Error: {e}")
        return pd.DataFrame()

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def test_customer_data():
    """Test the Customer table for correct data."""
    print("Testing Customer Data...")
    
    # Query to fetch data from the Customer table
    query = "SELECT * FROM Customer"
    df = fetch_data(query)
    
    # Example validations
    assert not df.empty, "Customer table is empty"
    assert 'customer_id' in df.columns, "customer_id column is missing"
    assert 'customer_name' in df.columns, "customer_name column is missing"
    assert 'country' in df.columns, "country column is missing"
    assert 'city' in df.columns, "city column is missing"

    # Print the fetched data for verification
    print(df.head())

def test_order_item_relationship():
    """Test the relationship between Order and OrderItem tables."""
    print("Testing Order-OrderItem Relationship...")
    
    # Query to fetch data from Order and OrderItem tables
    orders_query = "SELECT order_id FROM Order"
    order_items_query = "SELECT order_id FROM OrderItem"
    
    orders_df = fetch_data(orders_query)
    order_items_df = fetch_data(order_items_query)
    
    # Example validations
    assert not orders_df.empty, "Order table is empty"
    assert not order_items_df.empty, "OrderItem table is empty"
    
    # Check if all order_ids in OrderItem are present in Order
    missing_orders = order_items_df[~order_items_df['order_id'].isin(orders_df['order_id'])]
    assert missing_orders.empty, "OrderItem table contains order_ids not present in Order table"

    # Print results
    print(f"Missing Orders:\n{missing_orders}")

def main():
    # Run tests
    test_customer_data()
    test_order_item_relationship()

if __name__ == "__main__":
    main()


# In[ ]:




