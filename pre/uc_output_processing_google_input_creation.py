import requests
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from urllib.parse import urlparse
import numpy as np
from meg.configs import urljoin
import Setting_files.settings as settings
from meg.mactions import upload_file
from meg.msession import init_session
session, csrf_token = init_session()
BASE_API_END_POINT =settings.BASE_API_END_POINT
REQ_REPORT_END_POINT = urljoin(BASE_API_END_POINT, "request_report")
main_input_file_name=settings.DHCP_INPUT_FILE_NAME
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
                    AND "{table_name}".exact_match ILIKE '%' || nickmatches.nickname1 || '%'
                '''
        df = pd.read_sql_query(query, connection)
        connection.close()
        df.rename(columns={'nickname1': 'nickname_1'}, inplace=True)
        return df
    except Exception as ex:
        print(ex)
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
    # output_file=None
    # global table_name
    status_dict=request_download(upload_id)
    # if status_dict['status']=='success':
    #     download_id=status_dict['download_id']
    #     print(download_id)
    #     response=requests.get(download_id)
    #
    #     if response.status_code == 200:
    #         with open(upload_id+'.csv', 'wb') as file:
    #             file.write(response.content)
    #         print("File downloaded successfully.")
    #     else:
    #         print(f"Failed to download file. Status code: {response.status_code}")
    # else:print(status_dict['status'])
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y_%m_%d_%H")
    # #
    table_name = settings.DHCP_DATABASE_PRIFIX + formatted_datetime
    print(table_name)
    output_file=pd.read_csv(upload_id+'.csv',index_col=False).fillna('')
    connection=sql_connection()
    output_file.to_sql(table_name, connection, if_exists='replace', index=False)
    connection.dispose()
    return table_name
def create_output_frame():
    #creating output format
    main_input = pd.read_csv(main_input_file_name, index_col=False).fillna('')
    output_frame = pd.DataFrame(
        columns=["NPI_Hospital_ID","Hospital_ID",	"NPI",	"Website",	"Parent_Website", "First_Name",	"Middle_Name",	"Last_Name"	,
                 "Nick_Name", "Credential",	"Output_URL","Type_of_Match","Name_in_URL","People_Checker_Status",
                 "Input_URL","Idx","Formatted_string","Post_processing","Match_text"])
    output_frame['NPI_Hospital_ID'] = main_input['npi'].astype(str) + main_input['hospital_id'].astype(str)
    output_frame["Hospital_ID"]=main_input['hospital_id']
    output_frame["NPI"]=main_input['npi']
    output_frame["Website"]=main_input['website']
    output_frame["Parent_Website"]=main_input['parent_website']
    output_frame["First_Name"]=main_input['first_name']
    output_frame["Middle_Name"]=main_input['middle_name']
    output_frame["Last_Name"]=main_input['last_name']
    output_frame["Credential"]=main_input['credential']
    output_frame=output_frame.drop_duplicates(keep='first').fillna('')
    return output_frame
    # print(output_frame.head(30).to_string())
def output_process(uc_output_file_name,output_frame,table_name):
    #output file is in use
    uc_output_data=pd.read_csv(uc_output_file_name,index_col=False).fillna('')
    elements_to_remove = ['NOT_FOUND', 'FIRST_LAST_STARTS_WITH', 'LAST_NAME_ONLY','LAST_FIRST_EXACT_PART','PC_APP_ERROR']
    uc_data = uc_output_data[~uc_output_data['match_type'].isin(elements_to_remove)]
    uc_data=uc_data[~uc_data['output_url'].str.contains('forage-caching.s3')]
    uc_data = uc_data[~uc_data['output_url'].str.contains('chrome-error://chromewebdata/')]
    uc_data = uc_data[uc_data['output_url']!='error']
    uc_data = uc_data[uc_data['output_url']!='PAGE_CRASH']


    nick_data = get_nick_name(table_name)
    for i in uc_data.itertuples():
        print(i)
        Type_of_Match = ''
        People_Checker_Status=''
        Name_in_URL = ''
        Nick_Name=''
        contact_id =i.contact_id
        npi_hospital_id = i.npi_hospital_id
        npi_hospital_id = str(npi_hospital_id)
        output_frame.loc[output_frame['NPI_Hospital_ID'] == npi_hospital_id, 'Output_URL'] = i.output_url
        if i.match_type in ['FIRST_INIT_LAST_EXACT_PART','FIRST_LAST_EXACT','FIRST_LAST_EXACT_PART','LAST_FIRST_EXACT']:
            Type_of_Match='firstnamelastname'
            People_Checker_Status='Found'
        elif i.match_type in ['MIDDLE_LAST_EXACT']:
            Type_of_Match='middlenamelastname'
            People_Checker_Status = 'Found'
        elif i.match_type in ['NICK_DB_LAST']:
            Type_of_Match='nicknamelastname'
            People_Checker_Status = 'Found'

        output_frame.loc[output_frame['NPI_Hospital_ID'] == npi_hospital_id, 'Type_of_Match'] = Type_of_Match
        output_frame.loc[output_frame['NPI_Hospital_ID'] == npi_hospital_id, 'Match_text'] = i.exact_match


        output_frame.loc[output_frame['NPI_Hospital_ID'] == npi_hospital_id, 'People_Checker_Status'] = People_Checker_Status
        output_frame.loc[output_frame['NPI_Hospital_ID'] == npi_hospital_id, 'Input_URL'] = i.qpkey
        output_frame.loc[output_frame['NPI_Hospital_ID'] == npi_hospital_id, 'Post_processing'] = 'UC_DIRECT'
        ###### nick name
        if Type_of_Match=='nicknamelastname':
            nick_data_1 = nick_data[nick_data['first_name'].str.lower() == i.first_name.lower()]
            nick_data_2 = nick_data_1[
                nick_data_1['nickname_1'].str.lower().apply(lambda x: x.lower() in i.exact_match.lower())]
            if len(nick_data_2) != 0:
                Nick_Name = nick_data_2['nickname_1'].iloc[0]

                output_frame.loc[output_frame['NPI_Hospital_ID'] == npi_hospital_id, 'Nick_Name'] = Nick_Name
        else:output_frame.loc[output_frame['NPI_Hospital_ID'] == npi_hospital_id, 'Nick_Name'] = ''
        ###########  Name_in_URL

        if str(i.last_name).lower() in i.qpkey.lower():
            Name_in_URL='LN'

        if str(i.middle_name).lower()in i.qpkey.lower() and str(i.last_name).lower() in i.qpkey.lower():
            if len(str(i.middle_name)) > 2:
                Name_in_URL='MNLN'

        if str(i.first_name).lower() in i.qpkey.lower() and str(i.middle_name).lower() in i.qpkey.lower():
            if len(str(i.middle_name))>2:
                Name_in_URL = 'FNMN'

        if str(i.first_name).lower() in i.qpkey.lower():
            Name_in_URL='FN'

        if str(i.first_name).lower() in i.qpkey.lower() and str(i.last_name).lower() in i.qpkey.lower():
            Name_in_URL='FNLN'

        output_frame.loc[output_frame['NPI_Hospital_ID'] == npi_hospital_id, 'Name_in_URL'] = Name_in_URL
    output_frame = output_frame.fillna('')
    output_frame = output_frame.replace('NULL', '')
    output_frame = output_frame.replace('null', '')
    # output_frame
    output_frame.to_csv(settings.DHCP_OUTPUT_FILE_AFTER_UC,index=False)

def save_output_main_file():
    output_file = pd.read_csv(settings.DHCP_OUTPUT_FILE_AFTER_UC, index_col=False).fillna('')
    connection = sql_connection()
    output_file.to_sql(settings.DHCP_OUTPUT_FILE_AFTER_UC, connection, index=False)
    connection.dispose()
def redirection_creation():
    main_input = pd.read_csv(main_input_file_name, index_col=False).fillna('')
    redirection_file = pd.DataFrame(columns=["input_domain","output_domain"])
    input_domains = pd.concat([main_input['parent_website'], main_input['website']])
    input_domains=input_domains.drop_duplicates(keep='first')
    redirection_file['input_domain'] = input_domains
    redirection_file=redirection_file.fillna('')
    redirection_file=redirection_file[redirection_file['input_domain']!='']
    redirection_file['input_domain'] = redirection_file['input_domain'].apply(lambda x: 'http://'+str(x) if 'http' not in x else x)
    redirection_file['input_domain'] = redirection_file['input_domain'].apply(lambda x: urlparse(x).netloc)
    redirection_file['input_domain'] = redirection_file['input_domain'].str.replace('www.', '')
    redirection_file = redirection_file.drop_duplicates(keep='first')
    redirection_file.to_csv(settings.DHCP_REDIRECTION_FILE+'.csv', index=False)
def updating_redirection_file():
    output_file = pd.read_csv(settings.DHCP_OUTPUT_FILE_AFTER_UC, index_col=False).fillna('')
    redirection_file = pd.read_csv(settings.DHCP_REDIRECTION_FILE+'.csv', index_col=False).fillna('')
    output_file=output_file[output_file['People_Checker_Status']!='']

    output_file['Input_URL'] = output_file['Input_URL'].apply(lambda x: 'http://' + str(x) if 'http' not in x else x)
    output_file['input_domain']=output_file['Input_URL'].apply(lambda x: urlparse(x).netloc)

    output_file['input_domain'] = output_file['input_domain'].str.replace('www.', '')
    output_file=output_file.drop_duplicates(subset=['input_domain'],keep='first')
    output_dict = dict(zip(output_file['input_domain'], output_file['Output_URL']))
    redirection_file['output_domain'] = np.where(redirection_file['input_domain'].isin(output_dict.keys()),
                                                 redirection_file['input_domain'].map(output_dict),
                                                 redirection_file['output_domain'])

    redirection_file['output_domain'] = redirection_file['output_domain'].apply(lambda x: 'http://' + str(x) if 'http' not in x else x)
    redirection_file['output_domain']=redirection_file['output_domain'].apply(lambda x: urlparse(x).netloc)
    redirection_file['output_domain'] = redirection_file['output_domain'].str.replace('www.', '')
    redirection_file.to_csv(settings.DHCP_REDIRECTION_FILE+'.csv', index=False)
    rediection_file_name=settings.DHCP_REDIRECTION_FILE
    rediection_file_name='backup_1_'+rediection_file_name+'.csv'
    redirection_file.to_csv(rediection_file_name, index=False)
def google_input_creation():
    # output_file=pd.read_csv(settings.DHCP_OUTPUT_FILE_AFTER_UC,index_col=False).fillna('')
    output_file=pd.read_csv('DHCP_Processed_filter_pre_output__202410022307.csv',index_col=False).fillna('')

    redirection_output_file=pd.read_csv(settings.DHCP_REDIRECTION_FILE+'.csv',index_col=False).fillna('')
    redirection_output_file=redirection_output_file[redirection_output_file['output_domain']!='']
    not_working=output_file[output_file['People_Checker_Status']=='']
    google_input = pd.DataFrame(
        columns=['npi_hospital_id','company_id','contact_id','first_name','middle_name','last_name','nick_name',
                 'domain_data',
                 'qpkey','client_name','client_tag','s3ttl'])
    for i in ['Website','Parent_Website']:
        temp_data=pd.DataFrame(
        columns=['npi_hospital_id','company_id','contact_id','first_name','middle_name','last_name','nick_name','domain_data',
                 'qpkey','client_name','client_tag','s3ttl'])
        not_working_temp=not_working[not_working[i]!='']
        temp_data['npi_hospital_id'] = not_working_temp["NPI_Hospital_ID"]
        temp_data['company_id'] = not_working_temp["Hospital_ID"]
        temp_data['contact_id'] = not_working_temp["NPI"]
        temp_data['first_name'] = not_working_temp["First_Name"]
        temp_data['website'] = not_working_temp["Website"]
        temp_data['parent_website'] = not_working_temp["Parent_Website"]
        temp_data['credential'] = not_working_temp["Credential"]
        temp_data['middle_name'] = not_working_temp["Middle_Name"]
        temp_data['last_name'] = not_working_temp["Last_Name"]
        temp_data['nick_name'] = ''
        temp_data['domain_data']=not_working_temp[i]
        temp_data['domain_data'] = temp_data['domain_data'].apply(lambda x: 'http://' + str(x) if 'http' not in x else x)
        temp_data['domain_data']=temp_data['domain_data'].apply(lambda x: urlparse(x).netloc)
        temp_data['domain_data'] = temp_data['domain_data'].str.replace('www.', '')
        temp_data['client_name'] = "DHC"
        temp_data['client_tag'] = "DHC"
        temp_data['s3ttl'] = "30"
        temp_data['do_lg']='No'
        redirection_dict = dict(zip(redirection_output_file['input_domain'], redirection_output_file['output_domain']))
        temp_data['domain_data'] = np.where(temp_data['domain_data'].isin(redirection_dict.keys()),
                                                     temp_data['domain_data'].map(redirection_dict),
                                                     temp_data['domain_data'])
        temp_data['qpkey'] = 'https://www.google.com/search?q=site:' + temp_data['domain_data'] + '+' + \
                                temp_data['first_name'].str.replace(' ', '+') + '+' + temp_data['last_name'].str.replace(' ', '+')

        google_input=pd.concat([google_input,temp_data])

    google_input.reset_index(drop=True, inplace=True)
    google_input=google_input.drop_duplicates(keep='first')
    google_input=google_input[google_input['domain_data']!='']
    google_input=google_input.fillna('')
    google_input.to_csv(settings.DHCP_GOOGLE_INPUT_UC, index=False)

def file_upload():
    # google_input=pd.read_csv(settings.DHCP_GOOGLE_INPUT_UC,index_col=False).fillna('')
    # print(google_input)
    upload_id = upload_file(settings.DHCP_GOOGLE_INPUT_UC, 'GOOGLE')
    print(upload_id)
    return upload_id
