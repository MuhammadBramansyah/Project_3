import encodings
from itertools import groupby
from msilib import schema
from operator import index
from re import S
#from matplotlib.pyplot import axes
import pandas as pd
import numpy as np

import os
import json
#from psycopg2 import cursor
#from pyparsing import col

from requests import patch
from sqlalchemy import column, false
from sql.query import create_table_dim

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
    conn, cursor = postgre_auth.connect(conn_type = 'cursor')

    query_dim = create_table_dim(schema = schema)
    cursor.execute(query_dim)
    conn.commit()

    query_fact = create_table_fact(schema=schema)
    cursor.execute(query_fact)
    conn.commit()

    cursor.close()
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
def insert_dim_case(data):
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
    
    return df_tr
    
## insert_fact_province_daily
def insert_fact_province_daily(data,dim_case):
    column = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_result = ['date','province_id','status','total']

    data = data[column]
    data = data.melt(id_vars=["tanggal", "kode_prov"], var_name="status", value_name="total").sort_values(["tanggal", "kode_prov", "status"])
    data  = data.groupby(by = ['tanggal','kode_prov','status']).sum()
    data = data.reset_index()

    data.columns = column_result
    data['id'] = np.arange(1, data.shape[0]+1)

    dim_case = dim_case.rename({'id':'case_id'},axis = 1)
    data = pd.merge(data,dim_case, how = 'inner', on = 'status')

    data = data[['id','province_id','case_id','date','total']]

    return data 

## insert fact province month
def insert_fact_province_month(data,dim_case):
    
    column = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_result = ['month', 'province_id', 'status', 'total']
    
    data = data[column]
    data['tanggal'] = data['tanggal'].apply(lambda x :x[:7])
    data = data.melt(id_vars = ['tanggal', 'kode_prov'], var_name = 'status', value_name = 'total').sort_values(['tanggal', 'kode_prov', 'status'])
    data = data.groupby(by =['tanggal','kode_prov', 'status']).sum()
    data = data.reset_index()

    data.columns = column_result
    data['id'] = np.arange(1,data.shape[0]+1)

    dim_case = dim_case.rename({'id':'case_id'},axis = 1)
    data = pd.merge(data,dim_case, how = 'inner', on ='status')

    data = data[['id','province_id','case_id','month', 'total']]

    return data

## insert data fact province year
def insert_data_fact_province_year(data,dim_case):
    column = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_results = ['year', 'province_id', 'status', 'total']

    data = data[column]
    data['tanggal'] = data['tanggal'].apply(lambda x:x[:4])
    data = data.melt(id_vars = ['tanggal', 'kode_prov'], var_name = 'status', value_name = 'total').sort_values(['tanggal', 'kode_prov','status'])
    data = data.groupby(by = ['tanggal', 'kode_prov', 'status']).sum()
    data = data.reset_index()

    data.columns = column_results
    data['id'] = np.arange(1,data.shape[0]+1)

    dim_case = dim_case.rename({'id':'case_id'},axis = 1)
    data = pd.merge(data,dim_case, how = 'inner', on = 'status')

    data = data[['id','province_id', 'case_id', 'year', 'total']]

    return data 

## insert data District 
## insert data district monthly
def insert_data_district_month(data,dim_case):
    column = ["tanggal", "kode_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_results = ['month', 'district_id', 'status', 'total']

    data = data[column]
    data['tanggal'] = data['tanggal'].apply(lambda x :x[:7])
    data = data.melt(id_vars = ['tanggal', 'kode_kab'], var_name = 'status', value_name = 'total').sort_values(['tanggal','kode_kab','status'])
    data = data.groupby(by = ['tanggal', 'kode_kab', 'status']).sum()
    data = data.reset_index()

    data.columns = column_results
    data['id'] = np.arange(1,data.shape[0]+1)

    dim_case = dim_case.rename({'id':'case_id'},axis = 1)
    data = pd.merge(data,dim_case, how = 'inner', on = 'status')

    data = data[['id', 'district_id', 'case_id', 'month', 'total']]
    
    return data 

## insert data district year
def insert_data_district_year(data,dim_case):
    column = ["tanggal", "kode_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    column_results = ['year', 'district_id', 'status', 'total']

    data = data[column]
    data['tanggal'] = data['tanggal'].apply(lambda x:x[:4])
    data = data.melt(id_vars = ['tanggal', 'kode_kab'], var_name = 'status', value_name = 'total').sort_values(['tanggal','kode_kab','status'])
    data = data.groupby(by = ['tanggal', 'kode_kab', 'status']).sum()
    data = data.reset_index()

    data.columns = column_results
    data['id'] = np.arange(1,data.shape[0] + 1)

    dim_case = dim_case.rename({'id':'case_id'},axis = 1)
    data = pd.merge(data,dim_case, how = 'inner', on = 'status')

    data = data[['id','district_id', 'case_id','year', 'total']]

    return data

## insert data to data warehouse (postgresql)
def insert_data_raw_to_datawarehouse(schema):
    mysql_auth = MYSQL(credentials['mysql_lake'])
    engine, engine_conn = mysql_auth.connect()
    data = pd.read_sql(sql = 'bramansyah_raw_covid', con = engine)
    engine.dispose()

    column = ["tanggal", "kode_prov", "nama_prov", "kode_kab", "nama_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    data = data[column]

    dim_province = insert_data_dim_province(data)
    dim_district = insert_data_dim_district(data)
    dim_case = insert_dim_case(data)

    fact_province_daily = insert_fact_province_daily(data,dim_case)
    fact_province_monthly = insert_fact_province_month(data,dim_case)
    fact_province_year = insert_data_fact_province_year(data,dim_case)
    fact_district_monthly = insert_data_district_month(data,dim_case)
    fact_district_yearly = insert_data_district_year(data,dim_case)

    postgre_auth = PostgreSQL(credentials['postgresql_warehouse'])
    engine, engine_conn = postgre_auth.connect(conn_type = 'engine')

    dim_province.to_sql('dim_province', schema=schema, con = engine, index = False, if_exists='replace')
    dim_district.to_sql('dim_district', schema=schema, con = engine, index = False,if_exists='replace')
    dim_case.to_sql('dim_case', schema=schema, con = engine, index = False, if_exists= 'replace')

    fact_province_daily.to_sql('fact_province_daily', schema = schema, con = engine, index = False, if_exists='replace')
    fact_province_monthly.to_sql('fact_province_monthly', schema = schema, con = engine, index=False,if_exists='replace' )
    fact_province_year.to_sql('fact_province_yearly', schema=schema, con = engine, index=False, if_exists='replace')
    fact_district_monthly.to_sql('fact_district_monthly',schema=schema, con = engine, index = False, if_exists='replace')
    fact_district_yearly.to_sql('fact_district_yearly', schema=schema, con = engine, index=False,if_exists='replace')

    engine.dispose()

if __name__ == '__main__':
    insert_raw_data()
    create_star_schema(schema='bramansyah')
    insert_data_raw_to_datawarehouse(schema='bramansyah')
    