from sqlalchemy import create_engine
from dotenv import load_dotenv
from scripts.migrate_dataset import read_dataset_sheet_1, read_dataset_sheet_2
import os
from geopy.distance import geodesic
import pandas as pd
import time
from scripts.db_connection import connect_db, psycopg_connect_db
from sqlalchemy import text

TABLE_NAME = "circles_test"
INTERSECTIONS_TABLE_NAME = "circles_intersections"

# 1	Городское
# 2	Окружное
# 3	Районное
# 4	Шаговая доступность

# calculating circle radius by availability
def availability_meters(availability: int) -> int:
    if availability == 4:
        return 500
    elif availability == 3:
        return 1000
    elif availability == 2:
        return 3000
    elif availability == 1:
        return 5000
    else:
        return 0

# making sql table migration query
def create_table_query() -> str:
    return f'''
        DROP TABLE IF EXISTS {TABLE_NAME};

        CREATE TABLE {TABLE_NAME} (
            object_id bigint,
            neighbor_object_id bigint,
            availability double precision,
            distance double precision
        );

        DROP TABLE IF EXISTS {INTERSECTIONS_TABLE_NAME};

        CREATE TABLE {INTERSECTIONS_TABLE_NAME} (
            object_id bigint,
            neighbor_object_id bigint,
            availability double precision,
            distance double precision
        );
    '''

# calculating circles overlaps and saving to earlier created database
def calc_overlaps_query(availability: int) -> str:
    a_meters = availability_meters(availability)
    return f'''
        WITH obs AS (
            SELECT * FROM objects
            WHERE availability = {availability}
        )

        INSERT INTO {TABLE_NAME}
        SELECT o.object_id as object_id, ob.object_id as neighbor_object_id, o.availability as availability, ST_DistanceSphere(o.position, ob.position) as distance FROM obs as o
        JOIN obs as ob on true
        WHERE (o.availability = {availability}) AND (ST_DistanceSphere(o.position, ob.position) BETWEEN 10 AND {a_meters});
    '''

# adding circle column and filling it with circles polygons
def circles_column_add_and_fill() -> None:
    conn, cursor = psycopg_connect_db()

    cursor.execute('''
        ALTER TABLE "objects"
        ADD COLUMN IF NOT EXISTS "circle" geometry;
    ''')
    for availability in range(1, 5):
        radius = availability_meters(availability)
        cursor.execute(f'''
            UPDATE "objects"
            SET "circle" = ST_SetSRID(ST_Buffer(position::geography, {radius})::geometry, 4326)
            WHERE availability = {availability}
        ''')
    conn.commit()
    cursor.close()
    conn.close()

# entry point
def main():
    circles_column_add_and_fill()

if __name__ == "__main__":
    main()