import pandas as pd
from geojson import Point, Feature
import json
from utils.utils import column_names_to_snake
from db_connection import connect_db

def read_parks_dataset() -> pd.DataFrame:
    return pd.read_json('assets/parks.json', encoding='windows-1251')

# def form_latlangs(df: pd.DataFrame) -> pd.DataFrame:
#     df['center_point_lat'] = None
#     df['center_point_lng'] = None

#     for index, row in df.iterrows():
#         coordinates = row['geodata_center']
#         lng, lat = coordinates['coordinates']
#         df.at[index, 'center_point_lat'] = lat
#         df.at[index, 'center_point_lng'] = lng

#     df = df[df['center_lat'].notna()]
#     return df

def update_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = ['id'] + column_names_to_snake(list(df.columns)[1:])
    return df

if __name__ == "__main__":
    df = read_parks_dataset()
    df = update_column_names(df)

    df['geo_data'] = df['geo_data'].astype(str)
    df['geodata_center'] = df['geodata_center'].astype(str)

    engine = connect_db()
    df.to_sql('parks', engine, index=False, if_exists='replace')
    engine.execute(
            '''
            ALTER TABLE "parks"
            ADD COLUMN "center_position" geometry;
            ALTER TABLE "parks"
            ADD COLUMN "center_point_lat" DOUBLE PRECISION;
            ALTER TABLE "parks"
            ADD COLUMN "center_point_lng" DOUBLE PRECISION;

            UPDATE "parks"
            SET "center_position" = ST_GeomFromGeoJSON(geodata_center);
            UPDATE "parks"
            SET "center_point_lat" = ST_Y(geodata_center);
            UPDATE "parks"
            SET "center_point_lng" = ST_X(geodata_center);

            ALTER TABLE "parks"
            ADD COLUMN "polygon" geometry;
            UPDATE "parks"
            SET "polygon" = ST_GeomFromGeoJSON(geo_data);

            ALTER TABLE "parks"
            DROP "geodata_center";
            ALTER TABLE "parks"
            DROP "geo_data";
            '''
        )