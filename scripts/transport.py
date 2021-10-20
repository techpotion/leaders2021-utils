import pandas as pd
from utils.utils import column_names_to_snake
from db_connection import connect_db

def read_transport_dataset() -> pd.DataFrame:
    return pd.read_json('assets/transport.json', encoding='windows-1251')

if __name__ == "__main__":
    df = read_transport_dataset()
    df.columns = column_names_to_snake(list(df.columns))
    print(df.iloc[0])

