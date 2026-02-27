import pandas as pd
from database import engine
from sqlalchemy import Integer, String, Float, DATE


def final_csv_info():
    df = pd.read_csv('final.csv', encoding='utf-8', dtype=str)
    print('Первые 5 строк')
    print(df.head())
    print('Последние 5 строк')
    print(df.tail())
    print(f'Размер датафрейма {df.shape}')
    print('Информация о дф')
    print(df.info())
    for col in df.columns:
        print(f'{col} - {df[col].unique()}')
        print(f'{col} - {df[col].describe()}')
    print(f"Дубликатов: {df.duplicated().sum()}")


def clean_csv_and_load_to_db():
    df = pd.read_csv('final.csv', encoding='utf-8', dtype=str)
    df.columns = df.columns.str.lower()

    df = df[~df['territory_code'].isin(['TERRITORY_CODE', '\x1a'])]

    df['date_of_arrival'] = pd.to_datetime(df['date_of_arrival'], errors='coerce')
    df['territory_code'] = pd.to_numeric(df['territory_code'], errors='coerce')
    df['days_cnt'] = pd.to_numeric(df['days_cnt'], errors='coerce')
    df['visitors_cnt'] = pd.to_numeric(df['visitors_cnt'], errors='coerce')
    df['spent'] = pd.to_numeric(df['spent'], errors='coerce')

    df['home_region'] = df['home_region'].fillna('Не указан')
    df['home_city'] = df['home_city'].fillna('Не указан')
    df['gender'] = df['gender'].fillna('Не указан')
    df['age'] = df['age'].fillna('Не указан')
    df['income'] = df['income'].fillna('Не указан')
    df['spent'] = df['spent'].fillna(0)

    # Если хотите посмотреть на подготовленные данные, закоментируйте код ниже
    df.to_sql(
        'tourists',
        engine,
        if_exists='replace',
        index=False,
        chunksize=10000,
        dtype={
            'territory_code': Integer,
            'territory_name': String(255),
            'date_of_arrival': DATE,
            'trip_type': String(100),
            'visit_type': String(50),
            'home_country': String(100),
            'home_region': String(200),
            'home_city': String(200),
            'goal': String(200),
            'gender': String(20),
            'age': String(50),
            'income': String(50),
            'days_cnt': Integer,
            'visitors_cnt': Integer,
            'spent': Float
        }
    )


if __name__ == '__main__':
    final_csv_info()
