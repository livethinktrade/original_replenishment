import psycopg2
import pandas as pd
import pandas.io.sql as psql
from psycopg2 import pool
import numpy as np

from psycopg2.extensions import register_adapter, AsIs
from ..Import.data_insertion import sales_insert, salesupdate
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

from transform_kroger import kroger_transform
from transform_kvat import kvat_transform
from transform_safeway import safeway_denver_transform


def sales_update(store_type_input, file, transition_date_range, current_year, current_week, connection_pool):

    kroger = ['kroger_central', 'kroger_columbus', 'kroger_dallas', 'kroger_delta', 'kroger_central']

    # series of if statement used to determine which program to use to transform the data to the proper format.

    if store_type_input in kroger:

        salesdata = kroger_transform(file, store_type_input, transition_date_range, current_year, current_week)

    elif store_type_input == 'kvat':

        salesdata = kvat_transform(file, transition_date_range, current_year, current_week)

    elif store_type_input == 'safeway_denver':

        salesdata = safeway_denver_transform(file, transition_date_range, current_year, current_week)

    else:
        print('Update method is not established for this store')


    connection = connection_pool.getconn()




    salesdata_len = len(salesdata)
    i=0
    update = 0
    insert = 0
    inserted_list = []

    while i < salesdata_len:


        transition_date_range= salesdata.iloc[i,0]
        store_year= salesdata.iloc[i,1]
        store_week=	salesdata.iloc[i,2]
        store_number= salesdata.iloc[i,3]
        upc= salesdata.iloc[i,4]
        sales= salesdata.iloc[i,5]
        qty= salesdata.iloc[i,6]
        current_year=salesdata.iloc[i,7]
        current_week=salesdata.iloc[i,8]
        store_type=salesdata.iloc[i,9]



        duplicate_check = psql.read_sql(f"""
                                            SELECT * FROM SALES 
                                            WHERE store_year ={store_year} and 
                                                store_week = '{store_week}' and 
                                                store_number = {store_number} and 
                                                upc = '{upc}' and 
                                                store_type = '{store_type}'
                                        """, connection)



        duplicate_check_len = len(duplicate_check)


        if duplicate_check_len == 1:

            salesupdate(transition_date_range,
                        store_year,
                        store_week,
                        store_number,
                        upc,
                        sales,
                        qty,
                        current_year,
                        current_week,
                        store_type,
                        connection_pool)
            update+=1
        else:
            sales_insert(transition_date_range,
                        store_year,
                        store_week,
                        store_number,
                        upc,
                        sales,
                        qty,
                        current_year,
                        current_week,
                        store_type,
                        connection_pool)
            insert +=1
            inserted_list.append(i)



        i+=1

        connection.commit()

    connection_pool.putconn(connection)
    print('update:',update)
    print('insert:',insert)
