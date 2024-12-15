import pandas as pd
from sqlalchemy import create_engine
import logging
import sys
from config import DATABASE_CONFIG, DATABASE_CONFIG_2, CSV_FILES, LOG_FILE, QUERY, COLUMNS, COL_DROP, DATAFRAMES, LOAD_ORDER
###
import random
from datetime import datetime, timedelta
import random

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_db_engine(config):
    """
    Crea una conexion de motor a la base de datos MySQL
    """
    try:
        engine = create_engine(f"mysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}")
        logging.info("Conexion a la base de datos fue exitosa")
        return engine
    except Exception as e:
        logging.error(f"Error al conectar a la base de datos: {e}")
        sys.exit(1)


def read_sql(query, conn):
    """
    Lee la consulta de la base de datos MySQL
    """
    try:
        df =  pd.read_sql(query, con=conn) 
        logging.info("Lectura de la consulta fue exitosa")
        return df
    except Exception as e:
        logging.error(f"Error leyendo la consulta: {e}")
        sys.exit(1)
    


def read_csv(file_path):
    """
    Lee un archivo CSV y devuelve un DataFrame
    """
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Archivo {file_path} leido correctamente")
        return df
    except Exception as e:
        logging.error(f"Error al leer el archivo {file_path}: {e}")
        sys.exit(1)


def movies_award_transform(df,col_to_int,col_old,col_new):
    """
    Transforma la tabla movies_award
    1) Cambia o asegura que todos los datos de la columna movieID sean enteros
    2) Cambia el nombre de la colulma Aware por Award
    """
    try:
        df[col_to_int]=df[col_to_int].astype('int')
        df.rename(columns={col_old:col_new}, inplace=True)
        logging.info("Transformacion a movies_award ejecutada correctamente")
        return df
    except Exception as e:
        logging.error("Tranformacion a movies_award fallo")
        sys.exit(1)



def tranform_merge(df, cols_to_replace, col_to_drop):
    """
    Transforma la tabla movies_award
    1) Cambia o asegura que todos los datos de la columna movieID sean enteros
    2) Cambia el nombre de la colulma Aware por Award
    """
    try:
        df = df.rename(columns=cols_to_replace)
        df = df.drop(columns=col_to_drop)
        logging.info("Transformacion del merge correctamente")
        return df
    except Exception as e:
        logging.error("Tranformacion de tabla merge fallo")
        sys.exit(1)



def merge_table_cross(table1,table2,PK1,PK2):
    """    
    Une dos columnas de dos tablas distintas por el metodo "cross"
    1) table1:tabla arbitraria
    2) table2:tabla arbitraria
    3) PK1: columna arbitraria
    4) PK2: columna arbitraria
    """
    try:
        column1=table1[PK1]
        column2=table2[PK2]
        New_table=pd.merge(column1,column2, how="cross")
        logging.info("Transformacion del merge_cross correctamente")
        return New_table
    except Exception as e:
        logging.error("Tranformacion de tabla merge_cross fallo")
        sys.exit(1)


def gen_rating():
    # Generar un número aleatorio entre 0 y 5 con 1 solo decimal
    numero_aleatorio = round(random.uniform(0, 5), 1)
    # Mostrar el número aleatorio
    return numero_aleatorio

def gen_timestamp():
    # Generar un timestamp aleatorio dentro de un rango específico
    start_date = datetime(2024, 1, 15)
    end_date = datetime(2024, 4, 6)

    # Calcular un valor aleatorio entre start_date y end_date
    random_date = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))

    # Mostrar el timestamp aleatorio
    return random_date

def load_data(engine, table_name, df):
    """
    Cargar datos a MySQl dado un dataframe.
    """
    try:
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        logging.info(f"Datos cargados correctamente en la tabla {table_name}")
    except Exception as e:
        logging.error(f"Error al cargar los datos en la tabla {table_name}: {e}")
        sys.exit(1)




def main():
    #ETL dimension MOVIE#
    ####################
    #Conexión al motor de SQL con la base de datos 'db_movies_netflix_transact'
    engine = create_db_engine(DATABASE_CONFIG)
    conn = engine.connect()

    #movies_data guarda la consulta (extraccion)
    movies_data=read_sql(QUERY, conn)
    #transformacion 
    movies_data["movieID"]=movies_data["movieID"].astype('int')


    #Extraccion de datos de un archivo csv
    movies_award=read_csv(CSV_FILES['Awards_movie'])
    #Transformacion de datos
    movies_award=movies_award_transform(movies_award,'movieID','Aware','Award')

    #TRANSFORMACION: se hace el join entre ambas tablas
    movie_data=pd.merge(movies_data,movies_award, left_on="movieID", right_on="movieID")

    #TRANSFORMACION: 
    #Conexión al motor de SQL con la base de datos preparando para la cacrga a 'dW_netflix'
    engine = create_db_engine(DATABASE_CONFIG_2)
    conn = engine.connect()

    #Transformación del merge movie_data movies_award
    movie_data = tranform_merge(movie_data, COLUMNS, COL_DROP)
    DATAFRAMES['dimMovie'] = movie_data



    #ETL dimension USER#
    ####################
    #Extraccion de datos
    users = pd.read_csv(CSV_FILES['users'], sep='|')
    #Transformacion: reemplazar idUSER por userID
    users = users.rename(columns={'idUser': 'userID'})
    DATAFRAMES['dimUser']=users


    #ETL tabla de Hechos#
    #####################
    #Extraccion: Se creo el dataframe watchs_data que une las columnas 
    #de las primary key de los dataframe users y la tabla movies_data
    watchs_data=merge_table_cross(users,movies_data,'userID','movieID')
    #Añade columna rating: datos creados aleatoriamente
    watchs_data["rating"]=watchs_data["movieID"].apply(lambda x: gen_rating())
    #Añade collumna timestamp: datos creados aleatoriamente
    watchs_data["timestamp"]=watchs_data["userID"].apply(lambda x: gen_timestamp())
    
    #Transformación: guardar tabla en un diccionario 
    DATAFRAMES['FactWatchs']=watchs_data

    #CARGA (LOADS): Como el procedimiento es el mismo para la tabla de dimensiones
    #se recomienda iterar con un proceso for

    for table in LOAD_ORDER:
       load_data(engine, table, DATAFRAMES[table])

    logging.info("Pipeline de datos se ejecuto correctamente")

   


if __name__ == "__main__":
    main()

