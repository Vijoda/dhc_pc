import numpy as np
from meg.configs import urljoin
from meg.msession import init_session
import Setting_files.settings as settings
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from urllib.parse import urlparse
from keyword_code.keyword_adding import keyword_cloumns_adding
import requests
session, csrf_token = init_session()
BASE_API_END_POINT =settings.BASE_API_END_POINT
DATABASE_PRIFIX =settings.DHCP_DATABASE_PRIFIX
REQ_REPORT_END_POINT = urljoin(BASE_API_END_POINT, "request_report")

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
def request_download(upload_id):
   dict_dt = {'upload_id':upload_id}
   download_id = None
   rs  = session.post(REQ_REPORT_END_POINT,json=dict_dt)
   dict_data = dict()

   if rs.status_code == 200:
       dict_data['status'] = 'success'
       dict_data['download_id'] = rs.text
   else:
       dict_data['status'] = 'fail'

   return dict_data
def start_download_and_process(upload_id=None):
    status_dict=request_download(upload_id)
    if status_dict['status']=='success':
        download_id=status_dict['download_id']
        print(download_id)
        response=requests.get(download_id)

        if response.status_code == 200:
            with open(upload_id+'.csv', 'wb') as file:
                file.write(response.content)
            print("File downloaded successfully.")
        else:
            print(f"Failed to download file. Status code: {response.status_code}")
    else:print(status_dict['status'])
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H")
    #
    table_name = DATABASE_PRIFIX +'Google_Direct_output_'+ formatted_datetime
    print(table_name)
    output_file=pd.read_csv(upload_id+'.csv',index_col=False)
    connection=sql_connection()
    output_file.to_sql(table_name, connection, if_exists='replace', index=False)
    connection.dispose()
    return table_name

global count

count=0
def finding_data_form_output(row,Proccessed_output_after_uc):
    global count
    count=count+1
    print(count)
    data_output=Proccessed_output_after_uc[Proccessed_output_after_uc['NPI_Hospital_ID'] == row]
    if data_output['Website'].iloc[0]:Website=data_output['Website'].iloc[0]
    else:Website=''
    if data_output['Parent_Website'].iloc[0]:Parent_Website=data_output['Parent_Website'].iloc[0]
    else:Parent_Website = ''
    if data_output['Credential'].iloc[0]:Credential=data_output['Credential'].iloc[0]
    else:Credential = ''
    return Website,Parent_Website,Credential
def find_input_url(row):
    input_url=row['Input_URL']
    Name_in_URL = ''
    firstname = row['First_Name']
    lastname = row['Last_Name']
    middle_name = row['Middle_Name']


    if str(lastname).lower() in input_url.lower():
        Name_in_URL = 'LN'

    if (str(middle_name).lower() in input_url.lower()) and (str(lastname).lower() in input_url.lower()):
        if len(str(middle_name)) > 2:
            # print(middle_name)
            Name_in_URL = 'MNLN'

    if (str(firstname).lower() in input_url.lower()) and (str(middle_name).lower() in input_url.lower()):
        if len(str(middle_name)) > 2:
            Name_in_URL = 'FNMN'

    if str(firstname).lower() in input_url.lower():
        Name_in_URL = 'FN'

    if (str(firstname).lower() in input_url.lower()) and (str(lastname).lower() in input_url.lower()):
        Name_in_URL = 'FNLN'

    return Name_in_URL

def database_connect():
    db_params = DATABASE_CREDENTIALS
    try:
        connection = psycopg2.connect(**db_params)
        return connection
    except Exception as ex:
        print(ex)
        return None

def get_nick_name(table_name):
    try:
        connection=database_connect()
        # query = f'''
        #     SELECT first_name, dhcnicknames2.nickname_1, exact_match
        #     FROM "{table_name}"
        #     JOIN dhcnicknames2 ON lower(dhcnicknames2.propername) = lower("{table_name}".first_name)
        #     AND "{table_name}".exact_match ILIKE '%' || dhcnicknames2.nickname_1 || '%'
        # '''
        query = f'''
                    SELECT "{table_name}".first_name, nickmatches.nickname1, "{table_name}".exact_match
                    FROM "{table_name}"
                    JOIN nickmatches ON lower(nickmatches.propername) = lower("{table_name}".first_name)
                    AND "{table_name}".exact_match ILIKE '%' || nickmatches.nickname1 || '%' AND match_type='NICK_DB_LAST'
                '''
        df = pd.read_sql_query(query, connection)
        connection.close()
        df.rename(columns={'nickname1': 'nickname_1'}, inplace=True)
        return df
    except Exception as ex:
        print(ex)

def find_nickname(row):
    print(555)
    Nick_Name=''
    if row['match_type']=='NICK_DB_LAST':
        nick_data_1 = nick_data[nick_data['first_name'].str.lower() == row['First_Name'].lower()]
        nick_data_2 = nick_data_1[
            nick_data_1['nickname_1'].str.lower().apply(lambda x: x.lower() in row['Match_text'].lower())]
        if len(nick_data_2) != 0:
            Nick_Name = nick_data_2['nickname_1'].iloc[0]
        return Nick_Name
def output_processing(table_name,upload_id=None):
    # only for getting column names
    global nick_data
    Proccessed_output_after_uc = pd.read_csv('dhcp_pc_run_output_final_202410171408.csv', index_col=False).fillna('')
    # Proccessed_output_after_uc = pd.read_csv(settings.DHCP_OUTPUT_FILE_AFTER_UC, index_col=False).fillna('')
    print(3333)
    elements_to_remove = ['NOT_FOUND', 'FIRST_LAST_STARTS_WITH', 'LAST_NAME_ONLY','LAST_FIRST_EXACT_PART',
                          'PC_APP_ERROR']
    output_file = pd.read_csv(upload_id + '.csv', index_col=False).fillna('')
    output_file = output_file[~output_file['match_type'].isin(elements_to_remove)]
    # output_file = output_file[~output_file['output_url'].str.contains('forage-caching.s3')]
    # output_file = output_file[~output_file['output_url'].str.contains('chrome-error://chromewebdata/')]
    # output_file = output_file[output_file['output_url'] != 'error']
    # output_file = output_file[output_file['output_url'] != 'PAGE_CRASH']
    # nick_data = get_nick_name(table_name)
    print(444)
    temp_frame = pd.DataFrame(columns=list(Proccessed_output_after_uc.columns))
    temp_frame['NPI_Hospital_ID'] = output_file['npi_hospital_id']
    temp_frame['Hospital_ID'] = output_file['company_id']
    temp_frame['NPI'] = output_file['contact_id']
    temp_frame['First_Name'] = output_file['first_name']
    temp_frame['Middle_Name'] = output_file['middle_name']
    temp_frame['Last_Name'] = output_file['last_name']
    temp_frame['Output_URL'] = output_file['output_url']
    temp_frame['match_type'] = output_file['match_type']
    temp_frame['Match_text'] = output_file['exact_match']
    temp_frame['Idx'] = output_file['Idx']
    temp_frame['Formatted_string'] = output_file['Formatted_string']
    temp_frame['Post_processing'] = 'UC_GOOGLE'
    temp_frame['Input_URL']=output_file['qpkey']

    print(999999)


    temp_frame[['Website', 'Parent_Website', 'Credential']] = temp_frame['NPI_Hospital_ID'].apply(
        lambda x: finding_data_form_output(x, Proccessed_output_after_uc)).apply(pd.Series)

    temp_frame['Name_in_URL'] = temp_frame.apply(find_input_url, axis=1)

    # temp_frame_nonick = temp_frame[temp_frame['match_type'] != 'NICK_DB_LAST']
    # temp_frame_nonick['Nick_Name']=''
    # temp_frame=temp_frame[temp_frame['match_type']=='NICK_DB_LAST']
    #
    #
    #
    # temp_frame['Nick_Name']=''
    # temp_frame['Nick_Name']=temp_frame.apply(find_nickname, axis=1)
    # print(555)
    # temp_frame=pd.concat([temp_frame_nonick,temp_frame])


    # type name matching
    temp_frame['Type_of_Match']=''
    temp_frame['Type_of_Match']=np.where(temp_frame['match_type'].isin(['FIRST_INIT_LAST_EXACT_PART', 'FIRST_LAST_EXACT',
                                        'FIRST_LAST_EXACT_PART', 'LAST_FIRST_EXACT']),
                                         'firstnamelastname',temp_frame['Type_of_Match'])
    temp_frame['Type_of_Match'] = np.where(
        temp_frame['match_type'].isin(['MIDDLE_LAST_EXACT']),
        'middlenamelastname', temp_frame['Type_of_Match'])
    temp_frame['Type_of_Match'] = np.where(
        temp_frame['match_type'].isin(['NICK_DB_LAST']),
        'nicknamelastname', temp_frame['Type_of_Match'])
    temp_frame['People_Checker_Status']='Found'
    print(6666)
    #remove data
    remove_list=list(temp_frame['NPI_Hospital_ID'])
    # Proccessed_output_after_uc=Proccessed_output_after_uc[~Proccessed_output_after_uc['NPI_Hospital_ID'].isin(remove_list)]
    # Proccessed_output_after_uc=pd.concat([temp_frame,Proccessed_output_after_uc])
    temp_frame.to_csv(settings.Dhcp_processed_output_after_google_uc,index=False)

    # Proccessed_output_after_uc.to_csv(settings.Dhcp_processed_output_after_google_uc,index=False)
    print(666)
def updating_redirection_file():
    output_file = pd.read_csv(settings.Dhcp_processed_output_after_google_uc, index_col=False).fillna('')
    redirection_file = pd.read_csv(settings.DHCP_REDIRECTION_FILE+'.csv', index_col=False).fillna('')
    output_file['Input_URL'] = output_file['Input_URL'].apply(lambda x: 'http://' + str(x) if 'http' not in x else x)
    output_file['input_domain'] = output_file['Input_URL'].apply(lambda x: urlparse(x).netloc)
    output_file['input_domain'] = output_file['input_domain'].str.replace('www.', '')

    output_file['output_domain'] = output_file['Output_URL'].apply(
        lambda x: 'http://' + str(x) if 'http' not in x else x)
    output_file['output_domain'] = output_file['output_domain'].apply(lambda x: urlparse(x).netloc)
    output_file['output_domain'] = output_file['output_domain'].str.replace('www.', '')

    output_file = output_file.drop_duplicates(subset=['input_domain'], keep='first')

    output_dict = dict(zip(output_file['input_domain'], output_file['output_domain']))

    # redirection_file['output_domain'] = np.where(redirection_file['input_domain'].isin(output_dict.keys()) and redirection_file['input_domain']=='',
    #                                              redirection_file['input_domain'].map(output_dict),
    #                                              redirection_file['output_domain'])
    redirection_file['output_domain'] = np.where(
        (redirection_file['input_domain'].isin(output_dict.keys())) & (redirection_file['output_domain'] == ''),
        redirection_file['input_domain'].map(output_dict),
        redirection_file['output_domain']
    )
    redirection_file.to_csv(settings.DHCP_REDIRECTION_FILE+'.csv', index=False)

    connection = sql_connection()
    redirection_file.to_sql(settings.DHCP_REDIRECTION_FILE, connection, index=False)
    connection.dispose()
def output_processing_keyword():
    output_file = pd.read_csv(settings.Dhcp_processed_output_after_google_uc, index_col=False).fillna('')
    keyword_file_from_pre_processing=keyword_cloumns_adding(output_file)
    return keyword_file_from_pre_processing

def filter_and_save_post_processing_data(pre_processing):

    # possivite_pre_processing_working = pre_processing.loc[
    #     (pre_processing['negative_keywords'] == '') &
    #     (pre_processing['education_keywords'] == '') &
    #     (pre_processing['news_keywords'] == '')
    #     ]
    #
    # positive_list = possivite_pre_processing_working['NPI_Hospital_ID'].to_list()
    # # found_data = possivite_pre_processing_working[possivite_pre_processing_working['People_Checker_Status'] != '']
    # # found_data_list = found_data['NPI_Hospital_ID'].to_list()
    # # #
    # # possivite_pre_processing_working = possivite_pre_processing_working[
    # #     (possivite_pre_processing_working['People_Checker_Status'] != '') |
    # #     (~possivite_pre_processing_working['NPI_Hospital_ID'].isin(found_data_list))
    # #     ]
    # # connection = sql_connection()
    # # possivite_pre_processing_working.to_csv(str(settings.DHCP_GOOGLE_FILTER_OUTPUT) + '.csv', index=False)
    # # possivite_pre_processing_working.to_sql(settings.DHCP_GOOGLE_FILTER_OUTPUT, connection, index=False)
    # # connection.dispose()
    # negetive_pre_processing_working = pre_processing.loc[
    #     (pre_processing['negative_keywords'] != '') |
    #     (pre_processing['education_keywords'] != '') |
    #     (pre_processing['news_keywords'] != '')
    #     ]
    # #
    # only_negetive = negetive_pre_processing_working[~negetive_pre_processing_working['NPI_Hospital_ID'].isin(positive_list)]
    # only_negetive_cleaning_data = only_negetive[
    #     ['NPI_Hospital_ID', 'Hospital_ID', 'NPI', 'Website', 'Parent_Website', 'First_Name',
    #      'Middle_Name', 'Last_Name', 'Nick_Name', 'Credential']]
    # only_negetive_cleaning_data=only_negetive_cleaning_data.drop_duplicates(keep='first')
    #
    # only_negetive_list=only_negetive_cleaning_data['NPI_Hospital_ID'].to_list()
    # no_only_negetive_only=pre_processing[~pre_processing['NPI_Hospital_ID'].isin(only_negetive_list)]
    # filtered_data = pd.concat([no_only_negetive_only, only_negetive_cleaning_data])
    # connection = sql_connection()
    # filtered_data.to_csv(str(settings.DHCP_GOOGLE_FILTER_OUTPUT) + '.csv', index=False)
    # filtered_data.to_sql(settings.DHCP_GOOGLE_FILTER_OUTPUT, connection, index=False)
    # connection.dispose()

    connection = sql_connection()
    print(333)
    # filtered_data.to_csv(str(settings.DHCP_GOOGLE_FILTER_OUTPUT) + '.csv', index=False)
    pre_processing.to_sql(settings.DHCP_GOOGLE_FILTER_OUTPUT, connection, index=False)
    connection.dispose()






