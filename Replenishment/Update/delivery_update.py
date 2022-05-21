#loading delivery data into database

import psycopg2
import pandas as pd
import pandas.io.sql as psql
from psycopg2 import pool
import numpy as np

from psycopg2.extensions import register_adapter, AsIs
from ..Import.data_insertion import delivery_insert, deliveryupdate
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)



######FIRST SECTION IS USING THE CSV FILE FROM QB DELIVERY AND FORMATING FILE TO PROPER FORMAT TO LOAD DB#########

#this line will need to be implemented in the original function


def delivery_update(transition_date_range, deliv_input, connection_pool):

    connection = connection_pool.getconn()
    

    df = pd.read_csv(f'{deliv_input}.CSV')

    #drops uneccesary columns
    df = df.drop(columns=['Unnamed: 0','Item','U/M', 'Sales Price', 'Amount', 'Balance'])
    df = df.dropna()

    #filters out unnecesary qb types only invoice or credit memo
    invoice = df[df.Type == ('Invoice')]
    credits = df[df.Type == ('Credit Memo')]
    df = pd.concat([invoice, credits])
    df = df.sort_values(by =['Date'])

    # df1 = df['Memo'].str.split(n=1)
    df[['upc', 'memo']] = df.Memo.str.split('/',n=1, expand=True)
    df.pop('Memo')
    df.pop('memo')

    #adding transion column and renanimg columns
    df['transition_date_range'] = f'{transition_date_range}'


    #takes hyphens out of up column extracts numbers only to prevent any rows that include freight or other non item sales (ie sales signs)
    df['upc'] = df["upc"].str.replace("-","")
    df['upc'] = df.upc.str.extract('(\d+)')
    df = df.dropna()

    # filters out and rows that doesn't have 12 digits in the UPC column this will eliminate any data with POG in the orignal UPC column
    df['upc_len'] = df['upc'].str.len()
    df = df[df.upc_len == 12]
    df.pop('upc_len')


    #store numbers collumn added
    df['store'] = df.Name.str.extract('(\d+)')



    #getting the data to provide store_types collumn for database 

    if deliv_input == 'kvat':
        df[['store_type','letter1']] = df.Name.str.split(':',n=1, expand=True)
        df.pop('letter1')

    elif deliv_input == 'fresh_encounter':
        df[['store_type','letter1']] = df.Name.str.split(',',n=1, expand=True)
        df.pop('letter1')
    else: 

        df[['letter', 'store_type','letter1']] = df.Name.str.split(':',n=2, expand=True)
        df.pop('letter1')
        df.pop('letter')

    df.pop('Name')

    store_list = {
        'ACME MARKETS': 'acme',
        'JEWEL OSCO' : 'jewel',
        'KROGER CENTRAL' : 'kroger_central',
        'INTERMOUNTAIN DIVISION' : 'intermountain',
        'KROGER COLUMBUS' : 'kroger_columbus',
        'KROGER DALLAS' : 'kroger_dallas',
        'KROGER DELTA' : 'kroger_delta', 
        'KROGER MICHIGAN': 'kroger_michigan',
        'ALBERTSONS DENVER' : 'safeway_denver',
        'TEXAS DIVISION' : 'texas_division',
        'KVAT FOOD STORES' : 'kvat',
        'FRESH ENCOUNTER' : 'fresh_encounter'   

    }


    store_type = df.iloc[0,7]

    df['store_type'] =store_list[store_type]

    #renaming collumns and putting them in the right order

    df = df.rename(columns={
                        "Type": "type", 
                        "Date": "date",
                        'Qty': 'qty',
                        'Num': 'num'
                        })

    new_deliv_transform = df[['transition_date_range', 'type', 'date', 'upc', 'store', 'qty', 'store_type', 'num']]



    ######LOADING DATA INTO POSTGRES WITH PYTHON #########

    new_deliv_transform_length = len(new_deliv_transform)
    i=0
    update = 0
    insert = 0

    while i < new_deliv_transform_length:

        transition_date_range = new_deliv_transform.iloc[i,0]
        type = new_deliv_transform.iloc[i,1]
        date = new_deliv_transform.iloc[i,2]
        upc = new_deliv_transform.iloc[i,3]
        store = new_deliv_transform.iloc[i,4]
        qty = new_deliv_transform.iloc[i,5]
        store_type = new_deliv_transform.iloc[i,6]
        num = new_deliv_transform.iloc[i,7]




        duplicate_check = psql.read_sql(f"""
                                            SELECT * FROM DELIVERY 
                                            WHERE type ='{type}' and 
                                                date = '{date}' and 
                                                store = {store} and 
                                                upc = '{upc}' and 
                                                store_type = '{store_type}' and
                                                num = {num}
                                        """, connection)

        duplicate_check_len = len(duplicate_check)


        if duplicate_check_len == 1:
            deliveryupdate(transition_date_range, type, date, upc, store, qty, store_type, connection_pool)
            update+=1
        else:
            delivery_insert(transition_date_range,type, date , upc, store, qty, store_type, num, connection_pool)
            insert +=1


        
        i+=1

        connection.commit()
        
    connection_pool.putconn(connection)
    print('update:',update)
    print('insert:',insert)