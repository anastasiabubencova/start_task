import pandas as pd
import random
import string
from datetime import datetime
from pandas_msgpack import to_msgpack, read_msgpack

types = ['int', 'float', 'boolean', 'string']

def int_column(length) :

    column = []
    for i in range(length) :
        column.append(random.randint(-2147483648, 2147483647))
    return column

def float_column(length) :
    column = []
    for i in range(length) :
        column.append(random.uniform(-100, 100))
    return column

def boolean_column(length) :
    column = []
    for i in range(length) :
        column.append(bool(random.randint(0,1)))
    return column

def string_column(length) :
    column = []
    letters = string.ascii_lowercase
    for i in range(length) :
        column.append(''.join([random.choice(letters) for j in range(random.randint(0,10))]))
    return column

def column(column_type, length) :
    if types[column_type] == 'int' :
        return int_column(length)
    elif types[column_type] == 'float':
        return float_column(length)
    elif types[column_type] == 'boolean' :
        return boolean_column(length)
    elif types[column_type] == 'string' :
        return string_column(length)



ds = pd.DataFrame()
columns_num = random.randint(0, 100)
rows_num = random.randint(0, 100)
for i in range(columns_num) :
    type = random.randint(0, 3)
    ds['col' + str(i) + "_" + types[type]] = column(type, rows_num)




start_csv = datetime.now()
ds.to_csv('dataset.csv')
start_feather = datetime.now()
ds.to_feather('dataset.feather')
start_hdf = datetime.now()
ds.to_hdf('dataset.h5', key='df')
start_msgpack = datetime.now()
to_msgpack('dataset.msg', ds)
start_parquet = datetime.now()
ds.to_parquet('df.parquet.gzip', 'gzip')
start_pickle = datetime.now()
ds.to_pickle('dataset.pkl')
start_jay = datetime.now()
ds.to_jay("dataset.jay")
end_time = datetime.now()


result = pd.DataFrame()
result['file'] = ['csv', 'feather', 'hdf', 'msgpack', 'parquet', 'pickle', 'jay']
result['save_time'] = [(start_feather - start_csv).microseconds, (start_hdf - start_feather).microseconds,
                       (start_msgpack - start_hdf).microseconds, (start_parquet - start_msgpack).microseconds,
                       (start_pickle - start_parquet).microseconds, (start_jay - start_pickle).microseconds,
                       (end_time - start_jay).microseconds]



start_csv = datetime.now()
ds = pd.read_csv('dataset.csv')
start_feather = datetime.now()
ds = pd.read_feather('dataset.feather')
start_hdf = datetime.now()
ds = pd.read_hdf('dataset.h5', key='df')
start_msgpack = datetime.now()
read_msgpack('dataset.msg', ds)
start_parquet = datetime.now()
ds = pd.read_parquet('df.parquet.gzip', 'gzip')
start_pickle = datetime.now()
ds = pd.read_pickle('dataset.pkl')
start_jay = datetime.now()
ds = pd.read_jay("dataset.jay")
end_time = datetime.now()

result['load_time'] = [(start_feather - start_csv).microseconds, (start_hdf - start_feather).microseconds,
                       (start_msgpack - start_hdf).microseconds, (start_parquet - start_msgpack).microseconds,
                       (start_pickle - start_parquet).microseconds, (start_jay - start_pickle).microseconds,
                       (end_time - start_jay).microseconds]

print(result)