import Setting_files.settings as settings
from keyword_code.keyword_adding import keyword_cloumns_adding
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
load_dotenv()

DATABASE_CREDENTIALS = {
    'host': os.getenv("host"),
    'database': os.getenv("database"),
    'user': os.getenv("user"),
    'password': os.getenv("password"),
    'port': os.getenv("port")
}
def sql_connection():
    db_params=DATABASE_CREDENTIALS
    connection_string = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
    engine = create_engine(connection_string)
    return engine
def database_connect():
    db_params = DATABASE_CREDENTIALS
    try:
        connection = psycopg2.connect(**db_params)
        return connection
    except Exception as ex:
        print(ex)
        return None
def get_pre_proccesed_output(table_name):
    try:
        print(table_name)
        connection=database_connect()
        query = f'''SELECT * FROM "{table_name}"'''
        print(query)
        df = pd.read_sql_query(query, connection).fillna('')
        connection.close()
        return df
    except Exception as ex:
        print(ex)

def keyword_adding():
    output_file=get_pre_proccesed_output(settings.DHCP_GOOGLE_FILTER_OUTPUT)
    output_file=output_file.drop_duplicates(keep='first')
    keyword_file_from_pre_processing=keyword_cloumns_adding(output_file)
    # connection = sql_connection()
    # keyword_file_from_pre_processing.to_sql(settings.DHCP_GOOGLE_FILTER_OUTPUT, connection,if_exists='replace', index=False)
    # connection.dispose()


