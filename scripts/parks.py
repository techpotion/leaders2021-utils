import pandas as pd
from geojson import Point, Feature
import json
from utils.utils import column_names_to_snake
from db_connection import connect_db

def read_parks_dataset() -> pd.DataFrame:
    return pd.read_excel('assets/parks.xlsx')

def form_latlangs(df: pd.DataFrame) -> pd.DataFrame:
    df['center_lat'] = None
    df['center_lng'] = None

    for index, row in df.iterrows():
        coordinates = json.loads(row['geodata_center'])
        lng, lat = coordinates['coordinates']
        df.at[index, 'center_lat'] = lat
        df.at[index, 'center_lng'] = lng

    df = df[df['center_lat'].notna()]
    return df

def update_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = ['id'] + column_names_to_snake(list(df.columns)[1:])
    return df

if __name__ == "__main__":
    df = read_parks_dataset()
    df = form_latlangs(df)
    df = update_column_names(df)
    print(df.head(5))

    engine = connect_db()
    df.to_sql('parks', engine, index=False, if_exists='replace')
    engine.execute(
            '''
            ALTER TABLE "parks"
            ADD COLUMN "center_position" geometry;

            UPDATE "parks"
            SET "center_position"=ST_Point(center_lng, center_lat)
            '''
        )