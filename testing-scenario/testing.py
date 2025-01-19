from sqlalchemy import create_engine
import pandas as pd
import os
from pangres import upsert
from dotenv import load_dotenv

import sys
sys.path.append('..')

from helper.db_connector import dwh_engine

load_dotenv()

DIR_LOG = os.getenv("DIR_LOG")
DIR_DATASET = os.getenv("DIR_DATASET")
DIR_EXTRACT = os.getenv("DIR_EXTRACT")
DIR_TRANSFORM = os.getenv("DIR_TRANSFORM")
DIR_LOAD = os.getenv("DIR_LOAD")

# set up the database connection
engine = dwh_engine()

# # define the insert query
# insert_query = """
# INSERT INTO sales_data (product_name, main_category, sub_category, image, link, ratings, no_of_ratings, discount_price, actual_price)
# VALUES ('Testing Product', 'Testing Category', 'Testing Sub Category', 'https://sekolahdata-assets.s3.ap-southeast-1.amazonaws.com/notebook-images/mde-intro-to-data-eng/testing_image.png', 'https://pacmann.io/', 5, 30, 450, 1000);
# """

# define the data
data = {
    "product_name": ["Testing Product"],  # Unique identifier
    "main_category": ["Testing Category"],
    "sub_category": ["Testing Sub Category"],
    "image": ["https://sekolahdata-assets.s3.ap-southeast-1.amazonaws.com/notebook-images/mde-intro-to-data-eng/testing_image.png"],
    "link": ["https://pacmann.io/"],
    "ratings": [5],
    "no_of_ratings": [30],
    "discount_price": [450],
    "actual_price": [1000]
}


# convert the data to a pandas DataFrame
testing_data = pd.DataFrame(data)
testing_data.index.name = 'sales_id'
testing_data = testing_data.reset_index(drop=True)

# perform the upsert
try:
    # define the unique index as 'product_name'
    upsert(
        con=engine,
        df=testing_data,
        table_name="sales_data",
        if_row_exists="update"  # Options: 'update', 'ignore', 'error'
        # schema="public",  # Default schema for PostgreSQL
        # create_table=False  # Set to True if the table doesn't exist
    )
    print("Data successfully upserted into the sales_data table!")
except Exception as e:
    print(f"Error during upsert: {e}")