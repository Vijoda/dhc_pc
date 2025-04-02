import requests
from meg.msession import init_session
import Setting_files.settings as settings
from meg.configs import urljoin
from meg.mactions import upload_file
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

session, csrf_token = init_session()
BASE_API_END_POINT = settings.BASE_API_END_POINT
REQ_REPORT_END_POINT = urljoin(BASE_API_END_POINT, "request_report")
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
def request_download(upload_id):
    """
        request the download for the file
    """
    dict_dt = {'upload_id': upload_id}
    download_id = None
    rs = session.post(REQ_REPORT_END_POINT, json=dict_dt)
    dict_data = dict()

    if rs.status_code == 200:
        dict_data['status'] = 'success'
        dict_data['download_id'] = rs.text
    else:
        dict_data['status'] = 'fail'

    return dict_data
def start_download_and_process(upload_id=None):
    print(upload_id)
    status_dict = request_download(upload_id)
    if status_dict['status'] == 'success':
        download_id = status_dict['download_id']
        print(download_id)
        response = requests.get(download_id)

        if response.status_code == 200:
            with open(upload_id + '.csv', 'wb') as file:
                file.write(response.content)
            print("File downloaded successfully.")
        else:
            print(f"Failed to download file. Status code: {response.status_code}")
    else:
        print(status_dict['status'])
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H")
    #
    table_name = settings.DHCP_DATABASE_PRIFIX + formatted_datetime
    print(table_name)
    output_file = pd.read_csv(upload_id + '.csv', index_col=False)
    connection = sql_connection()
    output_file.to_sql(table_name, connection, if_exists='replace', index=False)
    connection.dispose()
def input_creation(upload_id=None):
        output=pd.read_csv(upload_id+'.csv',index_col=False).fillna('')
        output=output[output['direct_link']!='']
        # output = output[~output['direct_link'].str.endswith('.pdf')]
        output = output[~output['direct_link'].str.contains('.pdf')]
        output = output[~output['direct_link'].str.contains('wp-content')]
        direct_input=pd.DataFrame()
        direct_input['npi_hospital_id']=output['npi_hospital_id']
        direct_input['qpkey'] = output['direct_link']
        direct_input['company_id']=output['company_id']
        direct_input['website'] = output['website']
        direct_input['credential'] = output['credential']
        direct_input['parent_website'] = output['parent_website']
        direct_input['contact_id']=output['contact_id']
        direct_input['first_name']=output['first_name']
        direct_input['middle_name']=output['middle_name']
        direct_input['last_name']=output['last_name']
        direct_input['nick_name']=output['nick_name']
        direct_input['client_name']=output['client_name']
        direct_input['Idx']=output['idx']
        direct_input['Formatted_string']=output['formatted_string']
        direct_input['client_tag']=output['client_tag']
        direct_input['s3ttl']=output['s3ttl']
        direct_input['do_uc']='NOVPN_UH'
        direct_input['do_lg'] = 'NO'
        # direct_input=self.fillter_negetive_url(direct_input)
        direct_input.to_csv(settings.DHCP_UC_DIRECT_UPLOAD_FILE_NAME,index=False)

def file_upload():
    upload_id = upload_file(settings.DHCP_UC_DIRECT_UPLOAD_FILE_NAME, 'NOVPN_UH')
    print(upload_id)
    return upload_id