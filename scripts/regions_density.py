import pandas as pd
from db_connection import connect_db
from utils.utils import column_names_to_snake

def read_geojson() -> pd.DataFrame:
    df = pd.read_json('assets/moscow_regions.geojson')
    df['properties'] = df['properties'].apply(lambda x: x['NAME'])
    df = df.rename({'properties': 'region', 'geometry': 'geojson'}, axis=1)
    df = df[['region', 'geojson']]
    df['geojson'] = df['geojson'].astype(str)
    return df

def read_demography() -> pd.DataFrame:
    df = pd.read_excel('assets/demography.xlsx')
    df['District'] = df['District'].str.strip()
    return df

if __name__ == "__main__":
    df1 = read_demography()
    df2 = read_geojson()
    df = df1.merge(df2, left_on='District', right_on='region', how='right')
    df.columns = column_names_to_snake(list(df.columns))

    engine = connect_db()
    df.to_sql('regions', engine, index=False, if_exists='replace')
    engine.execute('''
        ALTER TABLE "regions"
        ADD COLUMN "polygon" geometry;
        ALTER TABLE "regions"
        ADD COLUMN "square" double precision;

        UPDATE "regions"
        SET "polygon" = ST_GeomFromGeoJSON(geojson);
        UPDATE "regions"
        SET "square" = ST_Area(polygon::geography);
    ''')
