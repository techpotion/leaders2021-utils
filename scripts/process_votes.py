import pandas as pd

def load_votes_df() -> pd.DataFrame:
    df = pd.read_csv('assets/votes.csv')
    return df

def load_stations() -> pd.DataFrame:
    df = pd.read_excel('assets/voting_stations.xlsx')
    df = df.loc[:, ['ID', 'Longitude_WGS84', 'Latitude_WGS84']]
    return df

def process_votes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.loc[df['УИК'] != 'Сумма']
    df = df.loc[df['Регион'] == 'город Москва']
    df = df.loc[:, ['УИК', 'Число избирателей, включенных в список избирателей']]
    df['УИК'] = df['УИК'].str.replace('УИК №', '')
    df['УИК'] = df['УИК'].astype('int64')
    return df

def merge_dfs(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    df = df2.merge(df1, left_on='УИК', right_on='ID')
    df = df.drop(columns=['УИК', 'ID'])
    return df

def main():
    df1 = load_stations()
    df2 = process_votes(load_votes_df())
    df = merge_dfs(df1, df2)
    df.to_excel('export/results.xlsx')

if __name__ == "__main__":
    main()