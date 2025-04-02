import pandas as pd
import Setting_files.credentials as credentials
import psycopg2
from os.path  import exists
import Setting_files.settings as settings
import uuid
import requests
from concurrent.futures import ThreadPoolExecutor
import csv
import re
import time
from sqlalchemy import create_engine
from .text_matching import get_matching
from psycopg2 import sql
from keyword_code.keyword_adding import keyword_cloumns_adding

def sql_connection():
    db_params=credentials.DATABASE_CREDENTIALS
    connection_string = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
    engine = create_engine(connection_string)
    return engine
def get_nick_name_match(firstname,lastname,text):
    nickname=''
    text_type=None
    found=None
    nickname=None
    text_data=None
    nick_data_1 = nick_data[nick_data['propername'].str.lower() == firstname.lower()]
    for n in nick_data_1.iterrows():
        nickname=n[1]['nickname_1']
        text_data=get_matching(text, fr"{re.escape(nickname)}.*?{re.escape(lastname)}",nickname,lastname)
        if text_data:
            return text_data,nickname
        # pattern = re.compile(fr"{re.escape(nickname)}.*?{re.escape(lastname)}", re.IGNORECASE | re.DOTALL)
        # match = re.search(pattern, text)
        # if match:
        #     text_data=match.group()
        #     break
    return text_data,nickname

def database_connect():
    db_params = credentials.DATABASE_CREDENTIALS
    try:
        connection = psycopg2.connect(**db_params)
        return connection
    except Exception as ex:
        print(ex)
        return None

def get_nick_name_data():
    try:
        db_params = {
                'host': 'forage-dev-db.cod4levdfbtz.ap-south-1.rds.amazonaws.com',
                'database': 'dhc',
                'user': 'postgres',
                'password': 'fhg37$76hGTy',
                'port': '5432'
            }
        query = f'''
            SELECT * from dhcnicknames2
        '''
        connection = psycopg2.connect(**db_params)
        df = pd.read_sql_query(query, connection)
        connection.close()
        return df
    except Exception as ex:
        print(ex)

def meta_work(input_file):
    if not exists('meta_google_addon.csv'):
        with open('meta_google_addon.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            # csv_writer.writerow(list(input_file.columns))
            csv_writer.writerow(['npi_hospital_id'])
            csvfile.close()

    meta_file = pd.read_csv('meta_google_addon.csv', index_col=False).fillna('')
    meta_file_list=meta_file['npi_hospital_id'].to_list()
    # working_input=input_file.merge(meta_file,how='outer')
    # working_input =pd.concat([input_file,meta_file]).drop_duplicates(keep=False)
    working_input=input_file[~input_file['npi_hospital_id'].isin(meta_file_list)]
    print(input_file)
    print(working_input)
    return working_input
# def creating_input(input_file,output_file):
#     output_file=output_file[output_file['People_Checker_Status']=='']
#     not_working_list=output_file['NPI_Hospital_ID'].to_list()
#     input_file=input_file[input_file['npi_hospital_id'].isin(not_working_list)]
#     print(input_file.to_string())
#     input_file=input_file[input_file['final_result'].isin(['WORKING_OLD','LESS_CONTENT','REDIRECTION_OKAY',
#                                                               'REDIRECTION_DOMAIN','REDIRECTION_PATH','REDIRECTION_QUERY'])]
#     input_file=input_file[~input_file['output_url'].str.contains('forage-caching.s3')]
#     input_file.to_csv(settings.DHCP_Google_direct_addon_input+'.csv',index=False)

def creating_input(input_file,output_file):
    output_file=output_file[output_file['Post_processing']!='UC_DIRECT']
    url_list=output_file['Output_URL'].to_list()
    # not_working_list=output_file['NPI_Hospital_ID'].to_list()
    # input_file=input_file[input_file['npi_hospital_id'].isin(not_working_list)]
    # print(input_file.to_string())
    input_file=input_file[~input_file['output_url'].isin(url_list)]
    input_file=input_file[input_file['final_result'].isin(['WORKING_OLD','LESS_CONTENT','REDIRECTION_OKAY',
                                                              'REDIRECTION_DOMAIN','REDIRECTION_PATH','REDIRECTION_QUERY'])]
    input_file=input_file[~input_file['output_url'].str.contains('forage-caching.s3')]
    input_file = input_file[~input_file['output_url'].str.contains('chrome-error://chromewebdata/')]
    input_file = input_file[input_file['output_url'] != 'error']
    input_file = input_file[input_file['output_url'] != 'PAGE_CRASH']
    input_file.to_csv(settings.DHCP_Google_direct_addon_input+'.csv',index=False)


def extract_visible_text(input_url,url, query_1,query_2,firstname,lastname,middle_name,row):
    try:
        text_type=None
        output_url=None
        nick_name_value=''
        text_data=''
        username = 'forager'
        password = 'vybKSMnN2R4GL9'
        response = requests.get(url, auth=(username, password))
        if response.status_code == 200:
            json_response = response.json()
            text_data = json_response['url_visible_text']

        visible_text=get_matching(text_data,query_1,firstname,lastname)
        print(response.status_code)
        if visible_text is not None:
            text_type='firstnamelastname'
        if visible_text == None:
            visible_text = get_matching(text_data, lastname+'.*?'+firstname, lastname, firstname)
            if visible_text is not None:
                text_type = 'firstnamelastname'
        if visible_text==None and query_2!=None :
                visible_text = get_matching(text_data, query_2,middle_name, lastname)
                if visible_text != None:
                    text_type = 'middlenamelastname'
        if visible_text == None:
                visible_text,nick_name_value = get_nick_name_match(firstname, lastname, text_data)
                if visible_text != None:
                    text_type='nicknamelastname'

        return visible_text,text_type,nick_name_value
    except Exception as ex:
        print(ex)
def insert_data(conn, table_name, data):
    print(table_name)
    print(data)
    # Extract column names from the first dictionary
    columns = data[0].keys()
    print(columns)
    # Create an SQL query for insertion
    insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )
    print(insert_query)
    try:
        with conn.cursor() as cursor:
            # Insert each dictionary in the list
            for record in data:
                cursor.execute(insert_query, tuple(record.values()))
        conn.commit()
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
def output_process(row,text_type,visible_text,nick_name_value):
    Post_processing='GOOGLE_DIRECT_ADDON'
    People_Checker_Status  = 'Found'
    Type_of_Match = text_type
    Input_URL =row['qpkey']
    output_URL = row['output_url']
    Match_text = visible_text
    company_id = row['company_id']
    contact_id = row['contact_id']
    website = row['website']
    parent_website = row['parent_website']
    credential = row['credential']
    first_name = row['first_name']
    middle_name=row['middle_name']
    last_name = row['last_name']
    NPI_Hospital_ID=row['npi_hospital_id']
    Name_in_URL=''

    nick_name=nick_name_value
    if str(last_name).lower() in Input_URL.lower():
        Name_in_URL = 'LN'

    if str(middle_name).lower() in Input_URL.lower() and str(last_name).lower() in Input_URL.lower():
        if len(str(middle_name)) > 2:
            Name_in_URL = 'MNLN'

    if str(first_name).lower() in Input_URL.lower() and str(middle_name).lower() in Input_URL.lower():
        if len(str(middle_name)) > 2:
            Name_in_URL = 'FNMN'

    if str(first_name).lower() in Input_URL.lower():
        Name_in_URL = 'FN'

    if str(first_name).lower() in Input_URL.lower() and str(last_name).lower() in Input_URL.lower():
        Name_in_URL = 'FNLN'

    data_list=[{"NPI_Hospital_ID":NPI_Hospital_ID,"Hospital_ID":company_id,"NPI":contact_id,"Website":website,
                "Parent_Website":parent_website,"First_Name":first_name,"Middle_Name":middle_name,"Last_Name":last_name,
                "Nick_Name":nick_name,"Credential":credential,"Output_URL":output_URL,"Type_of_Match":Type_of_Match,
                "Name_in_URL":Name_in_URL,"People_Checker_Status":People_Checker_Status,"Input_URL":Input_URL,
                "Post_processing":Post_processing,"Match_text":Match_text}]

    if len(data_list) > 0:
        conn = database_connect()
    else:
        conn = None
    if conn:
        # Insert data into the specified table
        insert_data(conn, post_output_file, data_list)
        conn.close()

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

def output_processing_keyword():
    output_file=get_pre_proccesed_output(settings.DHCP_GOOGLE_FILTER_OUTPUT)
    keyword_file_from_pre_processing=keyword_cloumns_adding(output_file)
    return keyword_file_from_pre_processing

def filter_and_save_post_processing_data(pre_processing):

    possivite_pre_processing_working = pre_processing.loc[
        (pre_processing['negative_keywords'] == '') &
        (pre_processing['education_keywords'] == '') &
        (pre_processing['news_keywords'] == '')
        ]

    found_data = possivite_pre_processing_working[possivite_pre_processing_working['People_Checker_Status'] != '']
    found_data_list = found_data['NPI_Hospital_ID'].to_list()
    #
    possivite_pre_processing_working = possivite_pre_processing_working[
        (possivite_pre_processing_working['People_Checker_Status'] != '') |
        (~possivite_pre_processing_working['NPI_Hospital_ID'].isin(found_data_list))
        ]

    # negetive_pre_processing_working = pre_processing.loc[
    #     (pre_processing['negative_keywords'] != '') |
    #     (pre_processing['education_keywords'] != '') |
    #     (pre_processing['news_keywords'] != '')
    #     ]
    #
    # only_negetive = negetive_pre_processing_working[~negetive_pre_processing_working['NPI_Hospital_ID'].isin(positive_list)]
    #
    # print(only_negetive)
    # only_negetive_cleaning_data = only_negetive[
    #     ['NPI_Hospital_ID', 'Hospital_ID', 'NPI', 'Website', 'Parent_Website', 'First_Name',
    #      'Middle_Name', 'Last_Name', 'Nick_Name', 'Credential']]
    #
    #
    # only_negetive_cleaning_data=only_negetive_cleaning_data.drop_duplicates(keep='first')
    #
    # only_negetive_list=only_negetive_cleaning_data['NPI_Hospital_ID'].to_list()
    #
    # no_only_negetive_only=pre_processing[~pre_processing['NPI_Hospital_ID'].isin(only_negetive_list)]
    #
    # filtered_data = pd.concat([no_only_negetive_only, only_negetive_cleaning_data])
    #
    # remove_process_duplicate_rows=filtered_data[filtered_data['People_Checker_Status']!='']
    # remove_process_duplicate_rows_list=remove_process_duplicate_rows['NPI_Hospital_ID'].to_list()
    # filtered_data=filtered_data[filtered_data['NPI_Hospital_ID'].isin(remove_process_duplicate_rows_list)&filtered_data['People_Checker_Status']=='']
    connection = sql_connection()
    possivite_pre_processing_working=possivite_pre_processing_working.drop_duplicates(keep='first')
    possivite_pre_processing_working.to_csv(str(settings.DHCP_GOOGLE_FILTER_OUTPUT)+'_addon' + '.csv', index=False)
    possivite_pre_processing_working.to_sql(settings.DHCP_GOOGLE_FILTER_OUTPUT, connection,if_exists='replace', index=False)
    connection.dispose()

def process_url(row, execution_id):
    npi_hospital_id=['npi_hospital_id']
    input_url=row['qpkey']
    url=row['s3status']
    if 'http' not in url:
        url='http://'+url
    npi_hospital_id=row['npi_hospital_id']
    firstname=row['first_name']
    lastname=row['last_name']
    middle_name=row['middle_name']
    query_1=firstname+'.*?'+lastname
    # print(query_1)
    if middle_name!=None and len(middle_name)>3:
        query_2 = middle_name + '.*?' + lastname
    else:query_2=None
    visible_text,text_type,nick_name_value = extract_visible_text(input_url,url, query_1,query_2,firstname,lastname,middle_name,row)
    if (text_type =='firstnamelastname') or (text_type=='middlenamelastname')or (text_type=='nicknamelastname'):
        # output_process(npi_hospital_id,text_type,url,output_url,firstname,middle_name,lastname,visible_text,nick_name_value)
        output_process(row,text_type,visible_text,nick_name_value)
    with open('meta_google_addon.csv', 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow([npi_hospital_id])
        csvfile.close()

def start_addon_search(input_file, output_file, num_threads):
    input_file = pd.read_csv(input_file + '.csv', index_col=False).fillna('')
    if len(input_file)>0:
        global nick_data
        global post_output_file
        post_output_file = settings.DHCP_GOOGLE_FILTER_OUTPUT
        nick_data = get_nick_name_data()
        working_input = meta_work(input_file)
        time.sleep(1)
        print(working_input)
        list_of_dicts = working_input.to_dict('records')
        execution_id = str(uuid.uuid4())
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            executor.map(lambda row: process_url(row, execution_id), list_of_dicts)