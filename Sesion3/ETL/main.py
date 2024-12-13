import pandas as pd
from sqlalchemy import create_engine
import logging
import sys
from config import DATABASE_CONFIG, DATABASE_CONFIG_2, CSV_FILES, LOG_FILE, QUERY, COLUMNS, COL_DROP

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



def main():
    #ETL dimension Movie
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

    movie_data = tranform_merge(movie_data, COLUMNS, COL_DROP)

   


if __name__ == "__main__":
    main()
