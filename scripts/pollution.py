import pandas as pd
from utils.utils import column_names_to_snake
from db_connection import connect_db

NORMAL_VALUE_SUBSTR = 'не превысило'

def read_polution_dataset() -> pd.DataFrame:
    return pd.read_json('assets/pollution.json', encoding='windows-1251')

if __name__ == "__main__":
    df = read_polution_dataset()
    df.columns = column_names_to_snake(list(df.columns))
    df['geo_data'] = df['geo_data'].astype(str)
    df['geodata_center'] = df['geodata_center'].astype(str)
    df['is_ok'] = True
    df = df.rename({'latitude__w_g_s84': 'center_point_lat', 'longitude__w_g_s84': 'center_point_lng'}, axis=1)

    for index, row in df.iterrows():
        df.at[index, 'is_ok'] = True if NORMAL_VALUE_SUBSTR in row['results'].lower() else False

    engine = connect_db()
    df.to_sql('pollution', engine, index=False, if_exists='replace')
    engine.execute(
        '''
        ALTER TABLE "pollution"
        ADD COLUMN "polygon" geometry;
        ALTER TABLE "pollution"
        ADD COLUMN "center_point" geometry;

        UPDATE "pollution"
        SET "polygon" = ST_GeomFromGeoJSON(geo_data);
        UPDATE "pollution"
        SET "center_point" = ST_SetSRID(ST_Point(center_point_lng, center_point_lat), 4326);

        ALTER TABLE "pollution"
        DROP "geo_data";
        '''
    )