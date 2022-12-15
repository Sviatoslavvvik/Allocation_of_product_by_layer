import sqlite3
import pandas as pd
import json


FILE_NAME = 'input/well_data.xlsx'
SPREAD1 = 'rates'
SPREAD2 = 'splits'
FILE_NAME_XLSX = 'input/result.xlsx'
FILE_NAME_JSON = 'input/result.json'


# создаем БД
conn = sqlite3.connect('well_data.db')
cur = conn.cursor()
cur.executescript('''
CREATE TABLE IF NOT EXISTS well(
    id INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS rates(
    id INTEGER PRIMARY KEY,
    dt TEXT,
    well_id INTEGER,
    oil_rate REAL,
    gas_rate REAL,
    water_rate REAL,
    FOREIGN KEY(well_id) REFERENCES well(id)
);

CREATE TABLE IF NOT EXISTS splits(
    id INTEGER PRIMARY KEY,
    dt TEXT,
    well_id INTEGER,
    layer_id INTEGER,
    oil_split REAL,
    gas_split REAL,
    water_split REAL,
    FOREIGN KEY(well_id) REFERENCES well(id)
);
''')
# импортирует данные из Excel
with pd.ExcelFile(FILE_NAME, engine='openpyxl') as xls:
    rates = pd.read_excel(xls, SPREAD1)
    splits = pd.read_excel(xls, SPREAD2)

# создаём таблицу с id скважин, для того чтобы можно было связать таблицы
well_id_list = [(i,) for i in range(300)]
well_id_list = list(well_id_list)

# заполняем БД
cur.executemany('INSERT INTO well VALUES(?);', well_id_list)
rates.to_sql("rates", conn, if_exists="append", index=False)
splits.to_sql("splits", conn, if_exists="append", index=False)
conn.commit()

# сохраняем данные в виде Excel файла

query = '''SELECT splits.dt,
       splits.well_id,
       splits.layer_id,
       (oil_split * oil_rate /100) AS oil_rate,
       (gas_split * gas_rate /100) AS gas_rate,
       (water_split * water_rate / 100) AS water_rate
FROM well
JOIN splits ON splits.well_id = well.id
JOIN rates ON rates.well_id = well.id
WHERE splits.dt = rates.dt '''

cur.execute(query)
index_col = ['dt', 'well_id', 'oil_rate', 'gas_rate', 'water_rate']
result = pd.read_sql_query(query, conn, index_col)
result.to_excel(FILE_NAME_XLSX, engine='openpyxl', merge_cells=False)

# сохраняем данные в виде json файла

query_result = [i for i in cur.fetchall()]
dict_json = {}
dict_json['allocation'] = {}
dict_json['allocation']['data'] = []
conn.commit()
cur.close()
with open(FILE_NAME_JSON, 'w') as the_file:
    for row in query_result:
        row_dict = {
            'wellId': row[1],
            'dt': row[0],
            'layerId': row[2],
            'oilRate': row[3],
            'gasRate': row[4],
            'waterRate': row[5]
        }
        dict_json['allocation']['data'].append(row_dict)
    json_data = json.dump(dict_json, the_file)
