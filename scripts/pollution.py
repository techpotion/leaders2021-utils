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
    df['is_polluted'] = False
    df = df.rename({'latitude__w_g_s84': 'center_point_lat', 'longitude__w_g_s84': 'center_point_lng'}, axis=1)

    for index, row in df.iterrows():
        df.at[index, 'is_polluted'] = False if NORMAL_VALUE_SUBSTR in row['results'].lower() else True

    engine = connect_db()
    df.to_sql('pollutions', engine, index=False, if_exists='replace')
    engine.execute(
        '''
        ALTER TABLE "pollutions"
        ADD COLUMN "polygon" geometry;
        ALTER TABLE "pollutions"
        ADD COLUMN "position" geometry;

        UPDATE "pollutions"
        SET "polygon" = ST_GeomFromGeoJSON(geo_data);
        UPDATE "pollutions"
        SET "position" = ST_SetSRID(ST_Point(center_point_lng, center_point_lat), 4326);

        ALTER TABLE "pollutions"
        DROP "geo_data";
        ALTER TABLE "pollutions"
        DROP "geodata_center";
        '''
    )