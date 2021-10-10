from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def connect_db():
    '''
    getting the database connection instance
    '''
    load_dotenv()

    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_username = os.getenv('DB_USERNAME')
    db_password = os.getenv('DB_PASSWORD')
    db_db = os.getenv('DB_DB')

    connection_string = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_db}'

    return create_engine(connection_string)