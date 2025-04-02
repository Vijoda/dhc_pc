import pandas as  pd
import psycopg2
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

def database_connect():
    db_params = DATABASE_CREDENTIALS
    try:
        connection = psycopg2.connect(**db_params)
        return connection
    except Exception as ex:
        print(ex)
        return None

def get_output_file(table_name):
    try:
        print(table_name)
        connection=database_connect()
        query = f'''SELECT "NPI_Hospital_ID","People_Checker_Status" FROM "{table_name}"'''
        print(query)
        df = pd.read_sql_query(query, connection).fillna('')
        connection.close()
        return df
    except Exception as ex:
        print(ex)

def input_create(Proccessed_output,dhcp_uc_input):
    working = Proccessed_output[Proccessed_output['People_Checker_Status'] != '']
    working_id_list=list(set(list(working['NPI_Hospital_ID'])))
    print(len(working_id_list))

    post_input = dhcp_uc_input[~dhcp_uc_input['npi_hospital_id'].isin(working_id_list)]
    post_input.to_csv('post_input.csv',index=False)

    # not_working = Proccessed_output[~Proccessed_output['NPI'].isin(working_id_list)]
    # final_output =pd.concat([working,not_working])
    # final_output.to_csv('Proccessed_output_after_google_uc.csv', index=False)


# dhcp_uc_input=pd.read_csv('Dhcp_uc_upload_input.csv',index_col=False)
# Proccessed_output_after_google_uc=pd.read_csv('Proccessed_output_after_google_uc.csv',index_col=False)
# Proccessed_output_after_google_uc = Proccessed_output_after_google_uc.fillna('')
# Proccessed_output_after_google_uc = Proccessed_output_after_google_uc.replace('NULL', '')
# Proccessed_output_after_google_uc = Proccessed_output_after_google_uc.replace('null', '')
#
# input_create(Proccessed_output_after_google_uc,dhcp_uc_input)