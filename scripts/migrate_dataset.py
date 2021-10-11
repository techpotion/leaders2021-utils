#
# dataset migration to the database
#

from db_connection import connect_db
import pandas as pd

def read_dataset_sheet_1() -> pd.DataFrame:
    '''
    reading dataset from excel file
    sheet 1 and processing it
    '''
    df: pd.DataFrame = pd.read_excel('assets/dataset.xlsx', 'Объекты отдельно')
    df = df.drop(columns=['Доступность.1'])
    df = df.rename({
        'id Объекта': 'object_id',
        'Объект': 'object_name',
        'Широта (Latitude)': 'object_point_lat',
        'Долгота (Longitude)': 'object_point_lng',
        'id Ведомственной Организации': 'departmental_organization_id',
        'Ведомственная Организация': 'departmental_organization_name',
        'Доступность': 'availability'
    }, axis='columns')

    return df

def read_dataset_sheet_2() -> pd.DataFrame:
    '''
    reading dataset from excel file
    sheet 2 and processing it
    '''
    df: pd.DataFrame = pd.read_excel('assets/dataset.xlsx', 'Со спортзонами и видами спорта')

    df = df.drop(columns=['Доступность.1', 'Unnamed: 12',  'Unnamed: 13', 'Unnamed: 14'])
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
        'Доступность': 'availability',
        'Вид спорта': 'sport_kind'
    }, axis='columns')

    return df

def migrate(drop=False, which='both') -> None:
    '''
    migrating it from dataframe
    right to the database
    '''
    engine = connect_db()

    if_exists = 'replace' if drop else 'append'

    if which == 'firsts' or 'both':
        df1 = read_dataset_sheet_1()
        df1.to_sql('objects', engine, index=False, if_exists=if_exists)

    if which == 'second' or 'both':
        df2 = read_dataset_sheet_2()
        df2.to_sql('objects_detailed', engine, index=False, if_exists=if_exists)

if __name__ == "__main__":
    migrate(drop=True, which='first')