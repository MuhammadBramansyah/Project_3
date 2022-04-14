from msilib import schema
from re import S
import pandas as pd
import numpy as np

import os
import json
from psycopg2 import cursor

from requests import patch
from Project_3.sql.query import create_table_dim

from script.mysql import MYSQL
from script.postgresql import PostgreSQL

from sql.query import create_table_dim, create_table_fact

## import credential
with open ('credential.json', 'r') as cred:
    credentials = json.load(cred)

## load data to data lake (MYSQL)
def insert_raw_data():
    mysql_auth = MYSQL(credentials['mysql_lake'])
    engine, engine_conn = mysql_auth.connect()

    path = os.getcwd()
    path_data = path + '/data/'

    data_source = 'data_covid.json'
    
    with open(path_data + f'{data_source}') as data_1:
        data_2 = json.load(data_1)

    data_results = []
    for i in data_2['data']['content']:
        data_results.append(i)
    
    df = pd.DataFrame(data_results)
    df.columns = df.columns.str.strip().str.lower()
    
    df.to_sql(name= 'bramansyah_raw_covid', con = engine, if_exists='replace', index=False)
    engine.dispose()

## create star schema 
def create_star_schema(schema):
    postgre_auth = PostgreSQL(credentials['postgresql_warehouse'])
    conn, cursor = postgre_auth.connnect(conn_type = 'cursor')

    query_dim = create_table_dim(schema = schema)
    cursor.execute(query_dim)
    conn.commit()

    query_fact = create_table_fact(schema=schema)
    cursor.execute(query_fact)
    conn.commit()

    cursor.colse()
    conn.close()    

## insert data dim province
def insert_data_dim_province(data):
    column = ['kode_prov','nama_prov']
    column_result = ['province_id', 'province_name']

    data = data[column]
    data = data.drop_duplicates(column)
    data.columns = column_result

    return data

## insert data dim_district
def insert_data_dim_district(data):
    column = ['kode_kab','kode_prov', 'nama_kab']
    column_result = ['district_id', 'province_id','district_name']

    data = data[column]
    data = data.drop_duplicates(column)
    data.columns = column_result

    return data

## insert data dim_case 
def insert_dim_case(data,df_tr):
    column = ["suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_result = ['id','status_name','status_detail','status']

    data = data[column]
    data = data[:1]

    df_tr = []
    for i in data:
        df_tr.append(i)

    df_tr = pd.DataFrame(df_tr,columns=['status'])
    df_tr = df_tr.sort_values('status')
    df_tr['id'] = np.arange(1, df_tr.shape[0]+1)
    df_tr[['status_name', 'status_detail']] = df_tr['status'].str.split('_', n=1,expand=True)
    df_tr = df_tr[column_result]
    
    return data,df_tr

## insert_fact_province_daily
def insert_fact_province_daily(data):
    column = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_result = ['id','province_id','case_id','date','total']

    data = data[column]
    
    



if __name__ == '__main__':
    insert_raw_data()
    