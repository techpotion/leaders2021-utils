import pandas as pd
from scripts.db_connection import connect_db

from scripts.utils.utils import column_names_to_snake

# reading moscow regions info dataset
def read_geojson() -> pd.DataFrame:
    df = pd.read_json('assets/moscow_regions.geojson')
    df['properties'] = df['properties'].apply(lambda x: x['NAME'])
    df = df.rename({'properties': 'region', 'geometry': 'geojson'}, axis=1)
    df = df[['region', 'geojson']]
    df['geojson'] = df['geojson'].astype(str)
    return df

# reading moscow density by regions dataset
def read_demography() -> pd.DataFrame:
    df = pd.read_excel('assets/regions.xlsx')
    df['density'] = df['density']
    df['population'] = df['population'].apply(lambda x: int(x[1:].replace(' ', '')))
    return df

# preprocessing data and pushing it to db
def push_to_db():
    df1 = read_demography()
    print(df1.head())
    df2 = read_geojson()
    df = df1.merge(df2, left_on='region', right_on='region', how='right')
    df.columns = column_names_to_snake(list(df.columns))
    df = df.dropna()
    engine = connect_db()
    df.to_sql('regions', engine, index=False, if_exists='replace')
    engine.execute('''
        ALTER TABLE "regions"
        ADD COLUMN "polygon" geometry;

        UPDATE "regions"
        SET "polygon" = ST_GeomFromGeoJSON(geojson);
    ''')

# entry point
def main():
    push_to_db()

if __name__ == "__main__":
    main()