import pandas as pd
import json
from scripts.db_connection import connect_db

def load_df() -> pd.DataFrame:
    with open('assets/metro.geojson', encoding='utf-8') as f:
        gj = json.load(f)
    df = pd.DataFrame(columns=['name', 'line_color', 'point_lat', 'point_lng'])
    for row in gj['features']:
        props = row['properties']
        geom = row['geometry']
        df = df.append({
            'name': props['TEXT'],
            'line_color': str(props['color']),
            'point_lat': geom['coordinates'][1],
            'point_lng': geom['coordinates'][0],
        }, ignore_index=True)
    return df

def df_to_db(df: pd.DataFrame):
    df = load_df()
    engine = connect_db()
    df.to_sql('subway_stations', engine, index=False, if_exists='replace')
    engine.execute('''
        ALTER TABLE "subway_stations"
        ADD COLUMN "position" geometry;

        UPDATE "subway_stations"
        SET "position" = ST_SetSRID(ST_Point(point_lng, point_lat), 4326)
    ''')

def main():
    df = load_df()
    df_to_db(df)

if __name__ == "__main__":
    main()