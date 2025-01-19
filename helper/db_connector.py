from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

# connection of data source
dbname = os.getenv("SOURCE_DB_NAME")
user = os.getenv("SOURCE_DB_USERNAME")
password = os.getenv("SOURCE_DB_PASSWORD")
host = os.getenv("SOURCE_DB_HOST")
port = os.getenv("SOURCE_DB_PORT")

# connection of data warehouse
dwh_dbname = os.getenv("WAREHOUSE_DB_NAME")
dwh_user = os.getenv("WAREHOUSE_DB_USERNAME")
dwh_password = os.getenv("WAREHOUSE_DB_PASSWORD")
dwh_host = os.getenv("WAREHOUSE_DB_HOST")
dwh_port = os.getenv("WAREHOUSE_DB_PORT")



def sales_db_engine():
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

    return engine

def dwh_engine():
    engine = create_engine(f"postgresql://{dwh_user}:{dwh_password}@{dwh_host}:{dwh_port}/{dwh_dbname}")

    return engine