import sqlite3
import pandas as pd

СOMPARISION_FLUID = {2: 'oil',
                     3: 'gas',
                     4: 'water'}


def generate_split_list(date_well):
    """Функция формирования списка вида:
    [дата, скважина, тип флюида с неверной суммой сплитов]"""
    date_well_fluids = [date_well[param] for param in range(2)]
    for index in range(2, 5):
        if date_well[index] != 100:
            date_well_fluids.append(СOMPARISION_FLUID.get(index))
    return date_well_fluids


FILE_NAME = 'input/well_data.xlsx'
SPREAD1 = 'invalid_splits'

if __name__ == '__main__':
    conn = sqlite3.connect('well_data.db')  # создаем БД
    cur = conn.cursor()
    # импортирует данные из Excel
    invalid_splits = pd.read_excel(FILE_NAME, SPREAD1, engine='openpyxl')
    # вставляем данные в БД
    invalid_splits.to_sql("invalid_splits", conn, if_exists="replace")
    conn.commit()
    cur.execute('''
    SELECT dt,
           well_id,
           SUM(oil_split) AS sum_oil_split,
           SUM(gas_split) AS sum_gas_split,
           SUM(water_split) AS sum_water_split
    FROM invalid_splits
    GROUP BY dt, well_id
    HAVING sum_oil_split != 100 OR sum_gas_split != 100
           OR sum_water_split != 100
    ''')  # получаем данные из БД с группировкой по дате и скважинам
    date_wel = cur.fetchall()  # преобразование в list
    answer = []
    for result in date_wel:
        answer.append(generate_split_list(result))
    conn.close()
    print(answer)
    