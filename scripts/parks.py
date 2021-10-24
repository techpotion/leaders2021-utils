import pandas as pd
from geojson import Point, Feature
import json
from scripts.utils.utils import column_names_to_snake
from scripts.db_connection import connect_db

# reading parks dataset from data.mos.ru
def read_parks_dataset() -> pd.DataFrame:
    return pd.read_json('assets/parks.json', encoding='windows-1251')

# column names processing
def update_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = ['id'] + column_names_to_snake(list(df.columns)[1:])
    return df

# preprocessing dataframe and pushing to db
def push_to_db():
        df = read_parks_dataset()
    df = update_column_names(df)

    df = df[['common_name', 'adm_area', 'district', 'location', 'has_sportground', 'geo_data', 'geodata_center']]

    df['geo_data'] = df['geo_data'].astype(str)
    df['geodata_center'] = df['geodata_center'].astype(str)
    df['has_sportground'] = df['has_sportground'].map({'да': True, 'нет': False})

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

# entry point
def main():
    push_to_db()

if __name__ == "__main__":
    main()