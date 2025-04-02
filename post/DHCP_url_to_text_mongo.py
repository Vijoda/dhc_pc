import sys
import time
import psycopg2
import pandas as pd
from bs4 import BeautifulSoup
from os.path  import exists
import uuid
import requests
from concurrent.futures import ThreadPoolExecutor
import csv
import re
from selenium import webdriver
from selenium.webdriver.common.proxy import Proxy, ProxyType
from .text_matching import get_matching
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
def meta_work(input_file):
    if not exists('meta_url_to_text.csv'):
        with open('meta_url_to_text.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(list(input_file.columns))
            csvfile.close()
    print(input_file.head(5).to_string())

    meta_file = pd.read_csv( 'meta_url_to_text.csv',dtype=str, index_col=False).fillna('')
    meta_file_list=meta_file['npi_hospital_id'].tolist()
    working_input = input_file[~input_file['npi_hospital_id'].isin(meta_file_list)]
    # print(meta_file.head(5).to_string())
    # working_input=input_file.merge(meta_file,how='outer')
    # working_input =pd.concat([input_file,meta_file]).drop_duplicates(keep=False)
    # print(input_file)
    return working_input
def extract_visible_text(url, query_1,query_2,firstname,lastname,middle_name,row):
    try:
        text_type=None
        output_url=None
        nick_name_value=''
        proxy=credentials.proxy
        proxy_url = proxy["server"].replace("https://", "").replace("http://", "")
        proxy_url = f"{proxy['username']}:{proxy['password']}@{proxy_url}"
        selenium_proxy = Proxy({
            'proxyType': ProxyType.MANUAL,
            'httpProxy': proxy_url,
            'sslProxy': proxy_url,
        })

        chrome_options = webdriver.ChromeOptions()
        chrome_options.Proxy = selenium_proxy

        browser = webdriver.Chrome(options=chrome_options)
        browser.get(url)
        time.sleep(3)

        output_url = browser.current_url
        soup = BeautifulSoup(browser.page_source, 'html.parser')
        text_data = soup.get_text()
        browser.close()
        # with sync_playwright() as p:
        #     browser = p.firefox.launch(headless=False)
        #     page = browser.new_page()
        #     page.goto(url, wait_until='load', timeout=120000)
        #     output_url=page.url
            # text_data=page.content()
            # soup = BeautifulSoup(page.content(), 'html.parser')
            # text_data = soup.get_text()
            # print(text_data)

        visible_text=get_matching(text_data,query_1,firstname,lastname)
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

        return visible_text,text_type,output_url,nick_name_value
    except Exception as ex:
        with open('error.csv', 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(row)
            csvfile.close()
        print(ex)
def get_nick_name_data():
    try:
        db_params = {
                'host': 'forage-dev-db.cod4levdfbtz.ap-south-1.rds.amazonaws.com',
                'database': 'dhc',
                'user': 'postgres',
                'password': 'fhg37$76hGTy',
                'port': '5432'
            }
        # query = "select first_name,dhcnicknames2.nickname_1,exact_match  from nasdaq_directurls_pc_q4_2023 join dhcnicknames2 on  lower(dhcnicknames2.propername) = lower(nasdaq_directurls_pc_q4_2023.first_name) and nasdaq_directurls_pc_q4_2023.exact_match ilike ''||dhcnicknames2.nickname_1||'%'"
        query = f'''
            SELECT * from dhcnicknames2
        '''
        connection = psycopg2.connect(**db_params)
        df = pd.read_sql_query(query, connection)
        # print(df)
        connection.close()
        return df
    except Exception as ex:
        print(ex)

def update_database_table(data_list, NPI_Hospital_ID):
    print(pre_proccesed_output)
    connection = database_connect()
    print(NPI_Hospital_ID)
    try:
        query = f'''
        UPDATE "{pre_proccesed_output}"
        SET "Output_URL" = %s, 
            "Type_of_Match" = %s, 
            "Name_in_URL" = %s, 
            "People_Checker_Status" = %s, 
            "Input_URL" = %s,
            "Post_processing" = %s, 
            "Match_text" = %s
        WHERE "NPI_Hospital_ID" = {NPI_Hospital_ID}
        '''
        cursor = connection.cursor()
        cursor.execute(query, tuple(data_list))
        connection.commit()
        cursor.close()
    except Exception as e:
        print(f"An error occurred: {e}")
        # Rollback in case of error
        connection.rollback()
    finally:
        # Close the connection
        connection.close()
def output_process(row,text_type,output_url,visible_text,nick_name_value):
    Post_processing='URL_TO_TEXT'
    People_Checker_Status  = 'Found'
    Type_of_Match = text_type
    Input_URL =row['qpkey']
    Output_URL = output_url
    Match_text = visible_text
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
    data_list=[Output_URL, Type_of_Match, Name_in_URL, People_Checker_Status, Input_URL, Post_processing,Match_text]
    update_database_table(data_list,NPI_Hospital_ID)
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
        query = f'''SELECT * FROM "{table_name}" where 'People_Checker_Status' !='Found'''
        print(query)
        df = pd.read_sql_query(query, connection).fillna('')
        connection.close()
        return df
    except Exception as ex:
        print(ex)
def process_url(row, execution_id):
    print(row)
    url=row['qpkey']
    if 'http' not in url:
        url='http://'+url
    npi_hospital_id=row['npi_hospital_id']
    firstname=row['first_name']
    lastname=row['last_name']
    middle_name=row['middle_name']
    query_1=firstname+'.*?'+lastname
    print(query_1)
    if middle_name!=None and len(middle_name)>3:
        query_2 = middle_name + '.*?' + lastname
    else:query_2=None
    visible_text,text_type,output_url,nick_name_value = extract_visible_text(url, query_1,query_2,firstname,lastname,middle_name,row)
    print(888)
    print(text_type)
    if (text_type =='firstnamelastname') or (text_type=='middlenamelastname')or (text_type=='nicknamelastname'):
        # output_process(npi_hospital_id,text_type,url,output_url,firstname,middle_name,lastname,visible_text,nick_name_value)
        output_process(row,text_type,output_url,visible_text,nick_name_value)
    data_list = [row['company_id'],
                row['contact_id'],
                row['npi_hospital_id'],
                row['first_name'],
                row['middle_name'],
                row['last_name'],
                row['nick_name'],
                row['qpkey'],
                row['client_name'],
                row['client_tag'],
                row['do_uc'],
                row['s3ttl'],
                row['input_type']]
    with open('meta_url_to_text.csv', 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(data_list)
        csvfile.close()
    # sys.exit()
def start_url_to_text(input_file,output_file,num_threads):
    global nick_data
    global pre_proccesed_output
    pre_proccesed_output=output_file
    nick_data = get_nick_name_data()
    working_input = meta_work(input_file)
    time.sleep(1)
    print(working_input)
    # sys.exit()
    # # list_of_lists = working_input.values.tolist()
    list_of_dicts = working_input.to_dict('records')
    # print(list_of_dicts)
    execution_id = str(uuid.uuid4())
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(lambda row: process_url(row, execution_id), list_of_dicts)