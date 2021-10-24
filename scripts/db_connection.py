from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import psycopg2

# making dsn connection string to db
def connection_string() -> str:
    load_dotenv()

    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_username = os.getenv('DB_USERNAME')
    db_password = os.getenv('DB_PASSWORD')
    db_db = os.getenv('DB_DB')

    return f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_db}'

# getting sqlalchemy db instance
def connect_db():
    '''
    getting the database connection instance
    '''
    return create_engine(connection_string())

# getting raw psycopg db instance
def psycopg_connect_db():
    conn = psycopg2.connect(connection_string())
    return conn, conn.cursor()