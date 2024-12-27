import pandas as pd
from tqdm import tqdm


def transform(profit_table: pd.DataFrame, date: str) -> pd.DataFrame:
    """Собирает таблицу флагов активности по продуктам
       на основании прибыли и количества совершённых транзакций.
       
       :param profit_table: pandas-датафрейм с данными транзакций.
       :param date: дата расчёта флагов активности в формате YYYY-MM-DD.
       :return: pandas-датафрейм с рассчитанными флагами.
    """
    # Проверка корректности даты
    try:
        date = pd.to_datetime(date)
    except ValueError:
        raise ValueError("Некорректный формат даты. Используйте YYYY-MM-DD.")
    
    # Подготовка диапазона дат
    start_date = date - pd.DateOffset(months=2)
    end_date = date + pd.DateOffset(months=1)
    
    # Приведение столбца `date` к формату datetime
    profit_table['date'] = pd.to_datetime(profit_table['date'])
    
    # Фильтрация по диапазону дат
    filtered_data = profit_table[
        (profit_table['date'] >= start_date) & (profit_table['date'] <= end_date)
    ]
    
    # Группировка и суммирование данных
    df_tmp = filtered_data.drop('date', axis=1).groupby('id').sum()
    
    # Создание флагов активности
    product_list = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
    for product in tqdm(product_list, desc="Processing flags"):
        df_tmp[f'flag_{product}'] = (
            (df_tmp[f'sum_{product}'] != 0) & (df_tmp[f'count_{product}'] != 0)
        ).astype(int)
    
    # Возврат только флагов
    df_tmp = df_tmp.filter(regex='flag').reset_index()
    return df_tmp


if __name__ == "__main__":
    # Чтение данных
    profit_data = pd.read_csv('profit_table.csv')
    
    # Расчёт флагов активности
    flags_activity = transform(profit_data, '2024-01-01')
    
    # Сохранение результата
    flags_activity.to_csv('flags_activity.csv', index=False)
