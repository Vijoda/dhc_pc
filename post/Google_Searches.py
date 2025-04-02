import sys
import pandas as pd
import psycopg2
import Setting_files.settings as settings
import numpy as np
import csv
import re
from bs4 import BeautifulSoup
import requests
import time
from os.path  import exists
import uuid
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.common.proxy import Proxy, ProxyType
from .text_matching import get_matching
# from test import get_matching
from psycopg2 import sql
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

def create_google_search_table():
    # Establish a connection to your PostgreSQL database
    conn = database_connect()
    # Create a cursor object
    cur = conn.cursor()
    # Step 1: Create the table if it doesn't exist
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS "DHCP_GOOGLE_SEARCH_URL" (
        id SERIAL PRIMARY KEY,
        qpkey VARCHAR,
        google_urls VARCHAR ,
        npi_hospital_id VARCHAR,
        idx VARCHAR,
        UNIQUE (id)
    );
    '''
    cur.execute(create_table_query)
    # Commit the transaction
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()
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
def get_redirection_output_file():
    try:
        connection=database_connect()
        query = f'''SELECT *  FROM "{settings.DHCP_REDIRECTION_FILE}"'''
        print(query)
        df = pd.read_sql_query(query, connection).fillna('')
        connection.close()
        print(df)
        return df
    except Exception as ex:
        print(ex)
def extract_domain(qpkey):
    parts = qpkey.split('q=site:')
    if len(parts) != 0:
        second_parts= parts[1].split('+')
        domian=second_parts[0]
        if second_parts[0].endswith('/'):
            domian=domian[:-1]
        return domian
    else:
        return None
def creating_input(input_file,output_file):
    input_file=pd.read_csv(input_file,index_col=False).fillna('')

    redirection_file=get_redirection_output_file()
    redirection_file=redirection_file[redirection_file['output_domain']!='']

    output_file=get_output_file(output_file)
    not_found=output_file[output_file['People_Checker_Status']=='']
    print(not_found.head(3).to_string())
    output_file_list = list(set(list(not_found['NPI_Hospital_ID'])))
    not_working_input=pd.DataFrame()
    print(input_file.head(3).to_string())
    # input_file = input_file[input_file['npi_hospital_id'].str.isin(output_file_list)]
    input_file = input_file[input_file['npi_hospital_id'].astype(str).isin(output_file_list)]

    print(input_file)
    redirection_dict = dict(zip(redirection_file['input_domain'], redirection_file['output_domain']))

    input_file['qpkey'] = np.where(input_file['domain_data'].isin(redirection_dict.keys()),
                                        input_file['domain_data'].map(redirection_dict),
                                        input_file['domain_data'])

    # input_file['qpkey'] = 'https://www.google.com/search?q=site:' + input_file['qpkey'] + '+' + \
    #         input_file['first_name'].str.replace(' ', '+') + '+' + input_file['last_name'].str.replace(' ','+')

    input_file.to_csv(settings.DHCP_Post_Google_search_input,index=False)

    return input_file
def meta_work(input_file):
    if not exists('meta_People_Checker_3.0.csv'):
        with open('meta_People_Checker_3.0.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(list(input_file.columns))
            csvfile.close()

    meta_file = pd.read_csv('meta_People_Checker_3.0.csv', index_col=False).fillna('')
    # working_input=input_file.merge(meta_file,how='outer')
    working_input =pd.concat([input_file,meta_file]).drop_duplicates(keep=False)
    print(input_file)
    print(working_input)
    return working_input
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


def update_google_search_url(url_list, npi_hospital_id, qpkey):
    try:
        conn = database_connect()
        cur = conn.cursor()
        insert_query = '''
            INSERT INTO "DHCP_GOOGLE_SEARCH_URL" (google_urls, npi_hospital_id, qpkey,idx)
            VALUES (%s, %s, %s, %s);
            '''

        # Iterate over the url_list and insert each URL into the database
        for index, url in enumerate(url_list):
            cur.execute(insert_query, (url, npi_hospital_id, qpkey,index))

        # Commit the transaction
        conn.commit()

        # Close the cursor and connection
        cur.close()
        conn.close()
        print(3434)
    except Exception as ex :
        print(ex)
        pass

def insert_data(conn, table_name, data):
    # Extract column names from the first dictionary
    columns = data[0].keys()

    # Create an SQL query for insertion
    insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )

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
def output_processing(results_list,firstname,lastname,middle_name,row):
    print('output_processing')
    try:
        temp_frame=pd.DataFrame(columns=['NPI_Hospital_ID','Hospital_ID','NPI','Website','Parent_Website','First_Name',
                                         'Middle_Name','Last_Name','Nick_Name','Credential','Output_URL','Type_of_Match',
                                         'Name_in_URL','People_Checker_Status','Input_URL','Formatted_string','Post_processing',
                                         'Match_text','match_type'])
        data_list=[]
        for r in results_list:
            if r['text_type']!=None:
                data_dict={}
                data_dict['NPI_Hospital_ID']=row['npi_hospital_id']
                data_dict['Hospital_ID'] = row['company_id']
                data_dict['NPI']=row['contact_id']
                data_dict['Website']=row['website']
                data_dict['Parent_Website']=row['parent_website']
                data_dict['Post_processing'] = 'PEOPLECHECKER'
                data_dict['First_Name']=row['first_name']
                data_dict['Middle_Name']=row['middle_name']
                data_dict['Last_Name']=row['last_name']
                data_dict['Credential']=row['credential']
                data_dict['Output_URL']=r['output_url']
                data_dict['Match_text'] = r['matched']
                data_dict['Type_of_Match'] = r['text_type']
                data_dict['People_Checker_Status'] = 'Found'
                data_dict['Input_URL'] = r['url']
                data_dict['Idx'] = ''
                data_dict['Formatted_string'] = ''
                input_url=r['url']

                Name_in_URL=''
                if str(lastname).lower() in input_url.lower():
                    Name_in_URL = 'LN'

                if (str(middle_name).lower() in input_url.lower()) and (str(lastname).lower() in input_url.lower()):
                    if len(str(middle_name)) > 2:
                        Name_in_URL = 'MNLN'

                if (str(firstname).lower() in input_url.lower()) and (str(middle_name).lower() in input_url.lower()):
                    if len(str(middle_name)) > 2:
                        Name_in_URL = 'FNMN'

                if str(firstname).lower() in input_url.lower():
                    Name_in_URL = 'FN'

                if (str(firstname).lower() in input_url.lower()) and (str(lastname).lower() in input_url.lower()):
                    Name_in_URL = 'FNLN'

                if Name_in_URL != None:
                    data_dict['Name_in_URL']=Name_in_URL

                if r['text_type']=='nicknamelastname':
                    data_dict['Nick_Name']=r['nick_name_value']
                else:data_dict['Nick_Name']=''

                data_list.append(data_dict)
        if len(data_list) > 0:
            conn = database_connect()
        else:
            conn=None
        if conn  :
            # Insert data into the specified table
            insert_data(conn, post_output_file, data_list)
            conn.close()
    except Exception as ex:
        print(ex)
def extract_visible_text(url, query_2,firstname,lastname,middle_name,query_1=None):
    try:
        text_type=None
        output_url=None
        nick_name_value = ''
        proxy = credentials.proxy
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
        time.sleep(4)
        output_url = browser.current_url
        soup = BeautifulSoup(browser.page_source, 'html.parser')
        text_data = soup.get_text()
        browser.quit()
        # with sync_playwright() as p:
        #     browser = p.firefox.launch(headless=False)
        #     page = browser.new_page()
        #     page.goto(url, wait_until='load', timeout=120000)
        #     output_url=page.url
        #     text_data=page.content()
        #     print(output_url)

        visible_text = get_matching(text_data, query_1, firstname, lastname)
        if visible_text is not None:
            text_type='firstnamelastname'
        print(11)
        if visible_text == None:
            visible_text = get_matching(text_data, lastname+'.*?'+firstname,lastname,firstname)
            text_type='firstnamelastname'
        print(222)
        if visible_text==None and query_2!=None :
            visible_text = get_matching(text_data, query_2,middle_name,lastname)
            if visible_text != None:
                text_type = 'middlenamelastname'
        print(3333)
        if visible_text == None:
            print('trying nick')
            visible_text,nick_name_value = get_nick_name_match(firstname, lastname, text_data)
            if visible_text != None:
                text_type='nicknamelastname'
            # if visible_text == None:
            #     visible_text = page.inner_text("body")
            #     text_type='Body'
            # # browser.close()
        return visible_text,output_url,text_type,nick_name_value
    except Exception as ex:
        print('inside error')
        print(ex)
def search_google(search_query_1, search_phrase_1, search_phrase_2, firstname, middle_name, lastname, npi_hospital_id,row):
    url = f"https://www.google.com/search?q=site:{search_query_1}"
    print(url)
    proxy_host = "proxy.zyte.com"
    proxy_port = "8011"
    proxy_auth = "d1d3dfa7dc4444a88a253a0263be5877:"  # Replace with your actual proxy credentials

    # Use HTTP scheme for the proxy URL for both HTTP and HTTPS
    proxies = {
        "http": f"http://{proxy_auth}@{proxy_host}:{proxy_port}",
        "https": f"http://{proxy_auth}@{proxy_host}:{proxy_port}"
    }

    response = requests.get(url, proxies=proxies, verify=False)  # Disable SSL certificate verification
    print(response.status_code)
    response.raise_for_status()
    soup = BeautifulSoup(response.text)
    selected_elements = soup.select('div.GyAeWb a[jsname="UWckNb"] ')
    url_list=[]
    if len(selected_elements)>0:
        for u in selected_elements:
            url_by_href=u.get('href')
            # if url_by_href:
            #     if not url_by_href.lower().endswith(".pdf") or not url_by_href.lower().endswith(".doc"):
            url_list.append(url_by_href)

    if len(url_list)>0:
        update_google_search_url(url_list,npi_hospital_id,row['qpkey'])

    url_list = [file for file in url_list if not file.lower().endswith(".doc")]
    url_list = [file for file in url_list if not file.lower().endswith(".pdf")]
    url_list = [file for file in url_list if not file.lower().endswith(".xml")]
    url_list = [file for file in url_list if not file.lower().endswith(".txt")]
    url_list = [file for file in url_list if not file.lower().endswith(".ashx")]
    url_list = [file for file in url_list if not file.lower().endswith(".docx")]
    url_list = [file for file in url_list if not file.lower().endswith(".xlsx")]
    url_list = [file for file in url_list if not ".xml?" in file]
    url_list = [file for file in url_list if not ".pdf?" in file]
    # url_list = [file for file in url_list if not "http://phppd.provide" in file]
    # url_list = [file for file in url_list if not "pulmonary" in file]
    # url_list = [file for file in url_list if not "cardio-heart/" in file]

    results_list = []
    for url in url_list:  # _with_newlines:
        print(url)
        try:
            visible_text, output_url, text_type, nick_name_value = extract_visible_text(url, search_phrase_2,firstname,lastname,middle_name,search_phrase_1)
        #
            if visible_text is not None and visible_text != "No match" and len(visible_text.split(' '))<=3 and text_type!='Body':
                # Add the result to the list
                results_list.append({'url': url, 'matched': visible_text,"output_url":output_url,"text_type":text_type,"nick_name_value":nick_name_value,
                    "npi_hospital_id":npi_hospital_id})
            # break
            # save_to_mongodb({'url': url, 'matched': visible_text,"input_url":input_url,"output_url":output_url,"text_type":text_type,
            #         "contact_id":contact_id,'UID':UID})
        except Exception as ex:
            print(ex)

    if len(results_list)>0:
        output_processing(results_list,firstname,lastname,middle_name,row)

    return

def filter_output(output_file):
    output_file_data=get_output_file(output_file)
    working_output=output_file_data[output_file_data['People_Checker_Status'] != '']
    working_output_list_1=list(set(working_output['NPI_Hospital_ID'].to_list()))
    working_output_list = [str(i) for i in working_output_list_1]
    # Convert list to string
    # id_string = ', '.join(map(str, working_output_list))
    id_string = ', '.join(f"'{item}'" for item in working_output_list)


    # SQL query
    query = f"""
    DELETE FROM "{output_file}"
    WHERE "NPI_Hospital_ID" IN ({id_string})
    AND "People_Checker_Status" != 'Found';
    """
    connection = database_connect()
    cur = connection.cursor()

    cur.execute(query)
    connection.commit()

    cur.close()
    connection.close()
def process_url(row, execution_id):
    search_phrase_2=None
    url=row['qpkey']
    npi_hospital_id=row['npi_hospital_id']
    firstname=row['first_name']
    lastname=row['last_name']
    middle_name=row['middle_name']

    search_query_1=url+' '+firstname+' '+lastname
    print(search_query_1)

    search_phrase_1=firstname+'.*?'+lastname

    if middle_name!=None and len(middle_name)>3:
        search_phrase_2=middle_name+'.*?'+lastname

    search_google(search_query_1, search_phrase_1, search_phrase_2, firstname, middle_name, lastname,npi_hospital_id,row)

    with open('meta_People_Checker_3.0.csv', 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(tuple(row.values()))
        csvfile.close()
    # sys.exit()
def start_google_search(input_file,output_file,num_threads):
    global nick_data
    global post_output_file
    post_output_file=output_file
    nick_data = get_nick_name_data()
    create_google_search_table()
    input_file=pd.read_csv(input_file,index_col=False).fillna('')
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