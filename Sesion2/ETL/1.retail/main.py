import pandas as pd
from sqlalchemy import create_engine
import logging
import sys
from config import DATABASE_CONFIG, CSV_FILES, LOG_FILE

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

def read_csv(file_path, columns):
    """
    Lee un archivo CSV y devuelve un DataFrame
    """
    try:
        df = pd.read_csv(file_path, header=None, sep='|', names=columns)
        logging.info(f"Archivo {file_path} leido correctamente")
        return df
    except Exception as e:
        logging.error(f"Error al leer el archivo {file_path}: {e}")
        sys.exit(1)

def transform_departments(df):
    """
    Realiza transformaciones en el dataframe departments
    """
    if df['department_name'].duplicated().any():
        logging.warning("Hay departamentos dupplicados en del DataFrame")
        sys.exit(1) 
    return df

def transform_customers(df):
    """
    Realiza transformaciones
    """
    df['customer_email'] = df['customer_email'].str.lower()
    # Validar campo obligatorios
    if df[['customer_fname', 'customer_lname', 'customer_email']].isnull().any().any():
        logging.warning("Datos faltantes en el DataFrame")
        sys.exit(1)
    return df

def validate_ids(df_retail, df, id_retail, id_df ):

    valid_ids = set(df[id_df])

    if not df_retail[id_retail].isin(valid_ids).all():
        logging.warning(f"hay {id_retail} que no se encuentran en el DataFrame")
        sys.exit(1)

def transform_products(df, df_categories):
    """
    Realiza transformaciones en DataFrame
    """

    validate_ids(df, df_categories, 'product_category_id', 'category_id' )

    return df

def transform_order_items(df, df_orders, df_products):
    """
    Realiza transformaciones
    """
    # order_item_order_id exista en orders
    validate_ids(df,df_orders, 'order_item_order_id', 'order_id' )

    # order_item_product_id exista en products
    validate_ids(df,df_products, 'order_item_product_id', 'product_id' )

    calculated_subtotal = df['order_item_quantity'] * df['order_item_product_price']
    if not (df['order_item_subtotal'] == calculated_subtotal ).all():
        df['order_item_subtotal'] = calculated_subtotal

    return df

def transform_orders(df, df_customers):
    """
    Realiza transformaciones específicas en el DataFrame de orders.
    """
    # Convertir order_date a datetime
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')

    if df['order_date'].isnull().any():
        logging.error("Hay valores inválidos en order_date.")
        sys.exit(1)
    # Asegurar que order_customer_id exista en customers
    validate_ids(df, df_customers, 'order_customer_id',  'customer_id' )

    return df

def load_data(engine, table_name, df):
    """
    """
    try:
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        logging.info(f"Datos cargados correctamente en la tabla {table_name}")
    except Exception as e:
        logging.error(f"Error al cargar los datos en la tabla {table_name}: {e}")
        sys.exit(1)

def main():

    engine = create_db_engine(DATABASE_CONFIG)

    load_order = ['departments', 'categories', 'customers', 'products', 'orders', 'order_items']
    dataframes = {}

    df_departments = read_csv(CSV_FILES['departments'], ['department_id', 'department_name'])
    df_departments = transform_departments(df_departments)
    dataframes['departments'] = df_departments
    
    df_categories = read_csv(CSV_FILES['categories'], ["category_id", "category_department_id", "category_name"])
    dataframes['categories'] = df_categories
    
    df_customers = read_csv(CSV_FILES['customers'], ["customer_id","customer_fname","customer_lname","customer_email","customer_password","customer_street","customer_city","customer_state","customer_zipcode"])
    df_customers = transform_customers(df_customers)
    dataframes['customers'] = df_customers

    df_products = read_csv(CSV_FILES['products'], ["product_id","product_category_id","product_name","product_description","product_price","product_image"])
    df_products = transform_products(df_products, df_categories)
    dataframes['products'] = df_products
    
    df_orders = read_csv(CSV_FILES['orders'], ["order_id","order_date","order_customer_id","order_status"])
    df_orders = transform_orders(df_orders, df_customers)
    dataframes['orders'] = df_orders

    df_order_items = read_csv(CSV_FILES['order_items'], ["order_item_id","order_item_order_id","order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price"])
    df_order_items = transform_order_items(df_order_items, df_orders, df_products)
    dataframes['order_items'] = df_order_items

    for table in load_order:
        load_data(engine, table, dataframes[table])

    logging.info("Pipeline de datos se ejecuto correctamente")


if __name__ == "__main__":
    main()

   

    

    