from sqlalchemy import create_engine
from dotenv import load_dotenv
from migrate_dataset import read_dataset_sheet_1, read_dataset_sheet_2
import os
from geopy.distance import geodesic
import pandas as pd
import time
from db_connection import connect_db, psycopg_connect_db
from sqlalchemy import text

TABLE_NAME = "circles_test"
INTERSECTIONS_TABLE_NAME = "circles_intersections"

# 1	Городское
# 2	Окружное
# 3	Районное
# 4	Шаговая доступность

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

if __name__ == "__main__":
    conn, cursor = psycopg_connect_db()

    cr_query = create_table_query()
    cursor.execute(cr_query)

    for i in range(1, 5):
        query = calc_overlaps_query(i)
        cursor.execute(query)

    conn.commit()
    cursor.close()
    conn.close()

