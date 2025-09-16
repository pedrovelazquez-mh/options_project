import pandas as pd
import warnings
warnings.simplefilter('ignore')
from datetime import datetime
import mysql.connector
from mysql.connector.pooling import PooledMySQLConnection
from mysql.connector.connection import MySQLConnection
from concurrent.futures import ThreadPoolExecutor
#from arcticdb import Arctic
#from credentials import ARCTIC_HOST
import sqlite3
from enum import Enum
from typing import Union, Any, List, Tuple
from datetime import datetime, date, timedelta
import pandas as pd
import psutil
from math import ceil
import sys
from credentials import SQL_HOST, SQL_USER, SQL_PASSWORD, SQL_DATABASE


carpeta = r"C:\Users\Pedro\Research\Options"
config_dir =carpeta+"\code"
if config_dir not in sys.path:
    sys.path.insert(0, config_dir)
import config_options as cfg
chosen_day = cfg.chosen_day


SYMBOLS_TABLE = "simbolo_oms"
MD_TABLE = "marketdata_oms"

class ObjectiveType(Enum):
    DATOS = 'datos'
    LAST_TRADES = 'last_trades'
    ARBITRAJE = 'arbitraje'

def sql_connect() -> Union[PooledMySQLConnection, MySQLConnection, Any]:
    return mysql.connector.connect(
        host = SQL_HOST,
        user = SQL_USER,
        password = SQL_PASSWORD,
        database = SQL_DATABASE,
        )

def get_all_symbols() -> pd.DataFrame:
    my_sql_connection = sql_connect()
    my_cursor = my_sql_connection.cursor()

    my_cursor.execute(f"SELECT * FROM {SYMBOLS_TABLE}")
    df = pd.DataFrame(my_cursor.fetchall(), columns=my_cursor.column_names)
    #df.to_csv('simbolos_id_all.csv')
    return df[['id','security_id']]

def get_symbols(symbol_name) -> pd.DataFrame:

    my_sql_connection = sql_connect()
    my_cursor = my_sql_connection.cursor()
    
    try:
        my_cursor.execute(f"SELECT * FROM {SYMBOLS_TABLE} WHERE security_id = '{symbol_name}'")
    except:
        my_cursor.execute(f"SELECT * FROM {SYMBOLS_TABLE} WHERE security_id IN{tuple(symbol_name)}")
    
    df = pd.DataFrame(my_cursor.fetchall(), columns=my_cursor.column_names)
    my_cursor.close()
    my_sql_connection.close()     
    return df[['id','security_id']]

def get_df(start_datetime: datetime, finish_datetime: datetime, list_of_symbol_id: List[int], objetivo: ObjectiveType) -> pd.DataFrame:
    my_sql_connection = sql_connect() 
    my_cursor = my_sql_connection.cursor()
    print('Query de conteo')   
    query_count=f"SELECT COUNT(*) FROM {MD_TABLE} WHERE fecha_insercion between '{start_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}' and '{finish_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}' and ultimo_precio <> 0"
    print(query_count)
    my_cursor.execute(query_count)
    print('Finaliza query de conteo')
    page_size=100000
    total_pages=int(ceil(my_cursor.fetchall()[0][0]/page_size))
    print('Total pages:',total_pages)
    def ejecutar_consulta(page_number):

        offset = (page_number - 1) * page_size
        if objetivo==ObjectiveType.DATOS:
            query=f"SELECT * FROM {MD_TABLE} WHERE fecha_insercion between '{start_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}' and '{finish_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}' and ultimo_precio <> 0 LIMIT {page_size} OFFSET {offset}"
        elif objetivo==ObjectiveType.LAST_TRADES:
            query=f"SELECT biof_fecha, ultimo_precio, id_simbolo FROM {MD_TABLE} WHERE fecha_insercion between '{start_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}' and '{finish_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}' and ultimo_precio <> 0 LIMIT {page_size} OFFSET {offset}"
        elif objetivo==ObjectiveType.ARBITRAJE:
            query=f"SELECT biof_fecha, ultimo_precio, id_simbolo, bi_1_precio, bi_1_size, of_1_precio, of_1_size FROM {MD_TABLE} WHERE fecha_insercion between '{start_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}' and '{finish_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}' and ultimo_precio <> 0 LIMIT {page_size} OFFSET {offset}"
        my_sql_connection = sql_connect() 
        my_cursor = my_sql_connection.cursor()
        print(query)
        my_cursor.execute(query)
        df2 = pd.DataFrame(my_cursor.fetchall(), columns=my_cursor.column_names)
        df2 = df2[df2.id_simbolo.isin(list_of_symbol_id)]

        my_cursor.close()
        my_sql_connection.close()

        return df2

    resultados_por_pagina=[]
    with ThreadPoolExecutor(max_workers=15) as executor:
        resultados_por_pagina = list(executor.map(ejecutar_consulta, (range(1, total_pages + 1))))

    resultado_final = pd.concat(resultados_por_pagina, ignore_index=True)   
    return resultado_final


def fetch_page(page_number: int, start_datetime: datetime, finish_datetime: datetime, objetivo: ObjectiveType, page_size: int, list_of_symbol_id: List[int]) -> pd.DataFrame:
    my_sql_connection = sql_connect()
    my_cursor = my_sql_connection.cursor()

    offset = (page_number - 1) * page_size
    columns = "biof_fecha, ultimo_precio, id_simbolo, bi_1_precio, bi_1_size, of_1_precio, of_1_size"
    if objetivo == ObjectiveType.DATOS:
        columns = "*"
    elif objetivo == ObjectiveType.LAST_TRADES:
        columns = "biof_fecha, ultimo_precio, id_simbolo"
    query = f"SELECT {columns} FROM {MD_TABLE} WHERE fecha_insercion BETWEEN %s AND %s AND ultimo_precio <> 0 LIMIT {page_size} OFFSET {offset}"

    my_cursor.execute(query, (start_datetime, finish_datetime))
    df = pd.DataFrame(my_cursor.fetchall(), columns=my_cursor.column_names)
    df = df[df.id_simbolo.isin(list_of_symbol_id)]

    my_cursor.close()
    my_sql_connection.close()
    return df

def trae_datos(start_datetime: datetime, finish_datetime: datetime, symbol_name, nombre_archivo: str, objetivo=ObjectiveType.DATOS, db='rubikia', n_steps=1):
    """
    Función que ejecuta la consulta SQL a las bases de datos.
    start_datetime: Fecha de inicio de la consulta en horario Argentina UTC-3.
    finish_datetime: Fecha de inicio de la consulta en horario Argentina UTC-3.
    symbol_name: nombres de los instrumentos en lista, separados por coma.
    nombre_archivo: nombre del archivo para guardar los datos recopilados como csv.
    objetivo: según el objetivo difiere las columnas que se traen de la DB, esto es para optimizar acorde a la necesidad. Pueden ser:
        'datos': trae todas las columnas para hacer análisis de orderbook & last_trades & ev, nv
        'last_trades': trae solo la información de las tres columnas que conforman el last_trade, para hacer un seguimiento del precio (por ejemplo para cálculos de volatilidad)
        'arbitraje': puede ser de utilidad obtener la primer cotización del oderbook, en ese caso va este objetivo. 
    db: 'rubikia' consulta a la DB de Rubikia (lento pero es donde están alojados los datos). 
    Si se pone 's3', consulta a la base de datos del disco Z datos_instrumentos, donde se aloja la misma información pero que anteriormente fue consultada y guardada ahí.
    Si se pone 'local', consulta a una base de datos del disco C. Solo disponible si se tiene una base alojada allí.

    n_steps: cantidad de divisiones de la consulta.
    """
    # se suma tres horas a la hora Argentina para convertir a UTC
    start_datetime+=timedelta(hours=3)
    finish_datetime+=timedelta(hours=3)
    
    step_size = (finish_datetime - start_datetime)/n_steps
 
    if db=='rubikia':
        # se obtienen los id de los símbolos
        print('Trayendo datos de símbolos')
        symbols=get_symbols(symbol_name)
        # se consulta con los id de los simbolos y el objetivo
        print('Trayendo datos')
        df=pd.DataFrame()
        for step in range(n_steps):
            print('Trayendo datos para step', step+1)
            try:
                df=pd.concat([df, get_df(start_datetime+step*step_size, start_datetime+(step+1)*step_size, symbols['id'].values, objetivo)])
             #   df.to_csv(nombre_archivo+'.csv')
            except:
                pass

        # se reemplaza el id del simbolo por su nombre
        print('Reemplazo ids')
        if len(df)>0:
            df['id_simbolo']=df['id_simbolo'].replace(symbols['id'].values, symbols['security_id'].values)
        
    elif db=='local'or db=='s3':

        if db=='local':
        #conexión a datos_instrumentos.db
            conexion = sqlite3.connect('C:\database\datos_instrumentos.db')
        elif db=='s3':
            conexion = sqlite3.connect('Z:\database\datos_instrumentos.db')
        
        cursor = conexion.cursor()
        try:
            cursor.execute(f"SELECT * FROM datos WHERE biof_fecha>='{start_datetime}' and biof_fecha<='{finish_datetime}' and id_simbolo = '{symbol_name}'")
        except:
            cursor.execute(f"SELECT * FROM datos WHERE biof_fecha>='{start_datetime}' and biof_fecha<='{finish_datetime}' and id_simbolo IN{tuple(symbol_name)}")

        df = pd.DataFrame(cursor.fetchall(), columns=['id','id_simbolo','ultimo_precio','ultimo_fecha','ultimo_size','bi_1_size','bi_2_size','bi_3_size','bi_4_size','bi_5_size','bi_1_precio','bi_2_precio','bi_3_precio','bi_4_precio','bi_5_precio','of_1_size','of_2_size','of_3_size','of_4_size','of_5_size','of_1_precio','of_2_precio','of_3_precio','of_4_precio','of_5_precio','biof_fecha','ev','nv'])
        conexion.close()
    else:
        raise Exception('Debe escribir db = rubikia, local o s3')

    # guarda db
   # df.to_csv(nombre_archivo+'.csv')
    return df


chosen_day = datetime.strptime(chosen_day, "%Y-%m-%d")

desde = chosen_day.replace(hour=9, minute=0, second=0)
hasta = chosen_day.replace(hour=18, minute=0, second=0)

simbolo='MERV - XMEV - GGAL - 24hs'
file_name='ggal.csv'
df=trae_datos(desde, hasta, simbolo, nombre_archivo=file_name, db='rubikia', n_steps=1)

# df.to_csv(file_name)
