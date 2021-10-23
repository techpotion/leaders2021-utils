#
# dataset migration to the database
#

from scripts.db_connection import connect_db
import pandas as pd

def read_dataset_sheet_1() -> pd.DataFrame:
    '''
    reading dataset from excel file
    sheet 1 and processing it
    '''
    df: pd.DataFrame = pd.read_excel('assets/dataset.xlsx', 'Объекты отдельно')
    print(df.columns)
    df = df.drop(columns=['Доступность.1'])
    df = df.rename({
        'id Объекта': 'object_id',
        'Объект': 'object_name',
        'Адрес': 'object_address',
        'Широта (Latitude)': 'object_point_lat',
        'Долгота (Longitude)': 'object_point_lng',
        'id Ведомственной Организации': 'departmental_organization_id',
        'Ведомственная Организация': 'departmental_organization_name',
        'Доступность': 'availability'
    }, axis='columns')
    print(df.head())
    print(df.columns)
    return df

def read_dataset_sheet_2() -> pd.DataFrame:
    '''
    reading dataset from excel file
    sheet 2 and processing it
    '''
    df: pd.DataFrame = pd.read_excel('assets/dataset.xlsx', 'Со спортзонами и видами спорта')
    print(df.columns)
    df = df.drop(columns=['Доступность.1', 'Unnamed: 13',  'Unnamed: 14', 'Unnamed: 15'])
    df = df.rename({
        'id Объекта': 'object_id',
        'Объект': 'object_name',
        'Широта (Latitude)': 'object_point_lat',
        'Долгота (Longitude)': 'object_point_lng',
        'id Ведомственной Организации': 'departmental_organization_id',
        'Ведомственная Организация': 'departmental_organization_name',
        'id Спортзоны': 'sports_area_id',
        'Спортзона': 'sports_area_name',
        'Тип спортзоны': 'sports_area_type',
        'Адрес': 'sports_area_address',
        'Площадь спортзоны': 'sports_area_square',
        'Доступность': 'availability',
        'Вид спорта': 'sport_kind',
    }, axis='columns')
    print(df.head())
    print(df.columns)
    return df

def migrate(replace=False, which='both') -> None:
    '''
    migrating it from dataframe
    right to the database
    '''
    engine = connect_db()

    if_exists = 'replace' if replace else 'append'

    if which == 'second' or 'both':
        df2 = read_dataset_sheet_2()
        df2 = df2.dropna()
        df2.to_sql('objects_detailed', engine, index=False, if_exists=if_exists)
        engine.execute(
            '''
            DELETE FROM "objects_detailed"
            WHERE object_point_lat IS NULL;

            ALTER TABLE "objects_detailed"
            ADD COLUMN "position" geometry;

            UPDATE "objects_detailed"
            SET "position" = ST_SetSRID(ST_Point(object_point_lng, object_point_lat), 4326)
            WHERE object_point_lng IS NOT NULL;
            '''
        )

    if which == 'firsts' or 'both':
        df1 = read_dataset_sheet_1()
        df1 = df1.dropna()
        df1.to_sql('objects', engine, index=False, if_exists=if_exists)
        engine.execute(
            '''
            DELETE FROM "objects"
            WHERE object_point_lat IS NULL;

            ALTER TABLE "objects"
            ADD COLUMN "position" geometry;

            UPDATE "objects"
            SET "position" = ST_SetSRID(ST_Point(object_point_lng, object_point_lat), 4326)
            WHERE object_point_lng IS NOT NULL;
            '''
        )
        engine.execute(
            '''
            ALTER TABLE "objects"
            ADD COLUMN "object_sum_square" double precision;

            WITH sumt as (
                SELECT object_id, SUM(sports_area_square) as total_square FROM "objects_detailed"
                GROUP BY object_id
            )

            UPDATE "objects" as o
            SET object_sum_square = (
                SELECT total_square FROM sumt
                WHERE object_id = o.object_id
            );

            DELETE FROM "objects"
            WHERE object_sum_square IS NULL
            '''
        )


def main():
    migrate(replace=True, which='first')

if __name__ == "__main__":
    main()