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
from psycopg2 import sql
import uuid
from .text_matching import get_matching
from concurrent.futures import ThreadPoolExecutor
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
        connection.close()
        return df
    except Exception as ex:
        print(ex)
def meta_work(input_file):
    if not exists('meta_google_search_blur.csv'):
        with open('meta_google_search_blur.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(list(input_file.columns))
            csvfile.close()

    meta_file = pd.read_csv('meta_google_search_blur.csv', index_col=False).fillna('')
    # working_input=input_file.merge(meta_file,how='outer')
    working_input =pd.concat([input_file,meta_file]).drop_duplicates(keep=False)
    print(input_file)
    print(working_input)
    return working_input
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
def output_processing(results, firstname, lastname, middle_name,npi_hospital_id,row):
    print('output_processing')
    time.sleep(3)
    try:
        data_list = []
        temp_frame = pd.DataFrame(
            columns=['NPI_Hospital_ID', 'Hospital_ID', 'NPI', 'Website', 'Parent_Website', 'First_Name',
                     'Middle_Name', 'Last_Name', 'Nick_Name', 'Credential', 'Output_URL', 'Type_of_Match',
                     'Name_in_URL', 'People_Checker_Status', 'Input_URL', 'Formatted_string', 'Post_processing',
                     'Match_text', 'match_type'])
        # working_row = post_processed_output.loc[post_processed_output['UID'] == UID]
        for r in results:
            nick_name_value=''
            text_type=None
            found=None
            if r['Blurb']:
                print('blur')
                text_data = get_matching(r['Blurb'], firstname+'.*?'+lastname,firstname, lastname)
                if text_data !=None:
                    text_type = 'firstnamelastname'
                    found = 'Found'
                if text_type == None:
                    text_data = get_matching(r['Blurb'], lastname + '.*?' + firstname,lastname, firstname)
                    if text_data:
                        text_type = 'firstnamelastname'
                        found = 'Found'

                if text_type == None and len(middle_name) > 3:
                    text_data = get_matching(r['Blurb'], middle_name+'.*?'+lastname, middle_name, lastname)
                    if text_data:
                        text_type = 'middlenamelastname'
                        found = 'Found'
                if text_type ==None:
                    text_data,nick_name_value = get_nick_name_match(firstname, lastname, r['Blurb'])
                    if text_data:
                        text_type = 'nicknamelastname'
                        found = 'Found'
                if text_type == None and len(lastname) >= 4:
                    text_data = get_matching(r['Blurb'], lastname, lastname, lastname)
                    nick_name_value = ''
                    if text_data:
                        text_type = 'lastname'
                        found = 'Maybe'
                if text_type:
                    print('inside_upend')
                    data_dict = {}
                    data_dict['Hospital_ID'] = row['company_id']
                    data_dict['NPI'] = row['contact_id']
                    data_dict['Website'] = row['website']
                    data_dict['Parent_Website'] = row['parent_website']
                    data_dict['First_Name'] = row['first_name']
                    data_dict['Middle_Name'] = row['middle_name']
                    data_dict['Last_Name'] = row['last_name']
                    data_dict['Credential'] = row['credential']
                    data_dict['Output_URL'] = r['URL']
                    data_dict['Type_of_Match'] = text_type
                    data_dict['Match_text'] = text_data
                    data_dict['People_Checker_Status'] = found
                    data_dict['Post_processing'] = 'GOOGLE_SEARCH'
                    data_dict['Idx'] = r['idx']
                    data_dict['Formatted_string'] = ''
                    data_dict['Input_URL'] = r['URL']
                    input_url = r['input_url']
                    data_dict['NPI_Hospital_ID']=row['npi_hospital_id']
                    output_url=r['URL']
                    Name_in_URL = ''
                    if str(lastname).lower() in output_url.lower():
                        Name_in_URL = 'LN'

                    if (str(middle_name).lower() in output_url.lower()) and (str(lastname).lower() in output_url.lower()):
                        if len(str(middle_name)) > 2:
                            Name_in_URL = 'MNLN'

                    if (str(firstname).lower() in output_url.lower()) and (str(middle_name).lower() in output_url.lower()):
                        if len(str(middle_name)) > 2:
                            Name_in_URL = 'FNMN'

                    if str(firstname).lower() in output_url.lower():
                        Name_in_URL = 'FN'

                    if (str(firstname).lower() in output_url.lower()) and (str(lastname).lower() in output_url.lower()):
                        Name_in_URL = 'FNLN'

                    if Name_in_URL != None:
                        data_dict['Name_in_URL'] = Name_in_URL
                    # Nick_Name = ''
                    # nick_data_1 = nick_data[nick_data['first_name'].str.lower() == firstname.lower()]
                    # nick_data_2 = nick_data_1[
                    #     nick_data_1['nickname_1'].str.lower().apply(lambda x: x.lower() in r['Blurb'].lower())]
                    # if len(nick_data_2) != 0:
                    #     Nick_Name = nick_data_2['nickname_1'].iloc[0]
                    data_dict['Nick_Name'] = nick_name_value
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

def get_output_file(table_name):
    try:
        connection=database_connect()
        query = f'''SELECT "NPI_Hospital_ID","People_Checker_Status" FROM "{table_name}"'''
        df = pd.read_sql_query(query, connection).fillna('')
        connection.close()
        return df
    except Exception as ex:
        print(ex)

def creating_input(input_file,output_file):
    input_file=pd.read_csv(input_file,index_col=False).fillna('')

    redirection_file=get_redirection_output_file()
    redirection_file=redirection_file[redirection_file['output_domain']!='']

    # output_file=get_output_file(output_file)
    # not_found=output_file[output_file['People_Checker_Status']=='']
    not_found = pd.read_csv('ids_3.csv', index_col=False).fillna('')
    output_file_list = list(set(list(not_found['NPI_Hospital_ID'])))
    print(output_file_list)
    not_working_input=pd.DataFrame()
    input_file = input_file[~input_file['npi_hospital_id'].isin(output_file_list)]

    redirection_dict = dict(zip(redirection_file['input_domain'], redirection_file['output_domain']))

    input_file['qpkey'] = np.where(input_file['domain_data'].isin(redirection_dict.keys()),
                                        input_file['domain_data'].map(redirection_dict),
                                        input_file['domain_data'])

    input_file.to_csv(settings.DHCP_Post_Blur_Google_search_input, index=False)

def filter_output(output_file):
    output_file_data=get_output_file(output_file)
    working_output=output_file_data[output_file_data['People_Checker_Status']!='']
    working_output_list=list(set(working_output['NPI_Hospital_ID'].to_list()))

    # Convert list to string
    # id_string = ', '.join(map(str, working_output_list))
    id_string = ', '.join(f"'{item}'" for item in working_output_list)


    # SQL query
    query = f"""
    DELETE FROM "{output_file}"
    WHERE "NPI_Hospital_ID" IN ({id_string})
    AND "People_Checker_Status" = '';
    """
    connection = database_connect()
    cur = connection.cursor()

    cur.execute(query)
    connection.commit()

    cur.close()
    connection.close()

def search(search_query,input_url,firstname,lastname,middle_name,npi_hospital_id,row):

    url = f"https://www.google.com/search?q={search_query}"
    print(url)
    proxy_host = "proxy.zyte.com"
    proxy_port = "8011"
    proxy_auth = "d1d3dfa7dc4444a88a253a0263be5877:"  # Replace with your actual proxy credentials

    # Use HTTP scheme for the proxy URL for both HTTP and HTTPS
    proxies = {
        "http": f"http://{proxy_auth}@{proxy_host}:{proxy_port}",
        "https": f"http://{proxy_auth}@{proxy_host}:{proxy_port}"
    }

    try:
        response = requests.get(url, proxies=proxies, verify=False)  # Disable SSL certificate verification
        response.raise_for_status()

        # Parse the HTML content with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # Use CSS selectors to extract URLs and blurbs
        links = soup.select('div.GyAeWb a[jsname="UWckNb"]')
        blurbs = soup.select('div.GyAeWb div[class="MjjYud"]')

        # Create a list to store the results
        results = []
        #Creating URL list
        # Url_list=[]
        # for link, blurb in zip(links, blurbs):
        #     url = link.get('href')
        #     Url_list.append(url)
        # if len(Url_list)>0:
        #     negetive_url_dict_list, negetive_url_list = keyword_filter(Url_list, keyword_data)
        #     if not exists('negetive_filtered_data.csv'):
        #         with open('negetive_filtered_data.csv', 'w', newline='') as csvfile:
        #             csv_writer = csv.writer(csvfile)
        #             csv_writer.writerow(['npi', 'qkey', 'keyword', 'time', 'Post_processing'])
        #             csvfile.close()
        #     for n in negetive_url_dict_list:
        #         with open('negetive_filtered_data.csv', 'a', newline='') as csvfile:
        #             csv_writer = csv.writer(csvfile)
        #             csv_writer.writerow([contact_id, n['url'], n['n_keyword'], datetime.now(), 'GOOGLE_SEARCH'])
        #             csvfile.close()

        # Add the extracted URLs and blurbs to the results list

        count = 0
        for link, blurb in zip(links, blurbs):
            print(blurb)
            url = link.get('href')
            blurb_text = blurb.get_text(strip=True)
            results.append({"input_url":input_url,"URL": url, "Blurb": blurb_text, "Query Parameter": search_query,'npi_hospital_id':npi_hospital_id,'idx':count})
            count = count + 1
        # Output Processing


        if len(results)>0:
            output_processing(results, firstname, lastname, middle_name,npi_hospital_id,row)
        # Insert the results into MongoDB
        #     save_to_mongodb(results)

    except Exception as ex:
        print(ex)
def process_url(row, execution_id):
    search_phrase_2=None
    url=row['qpkey']
    npi_hospital_id=row['npi_hospital_id']
    print(npi_hospital_id)
    firstname=row['first_name']
    lastname=row['last_name']
    if ' ' in lastname:
        lastname=lastname.replace(' ','-')
    middle_name=row['middle_name']
    query=firstname+'.*?'+lastname
    domain=url
    search_query='site:'+domain+'+'+firstname+'+'+lastname
    search(search_query,url,firstname,lastname,middle_name,npi_hospital_id,row)
    with open('meta_google_search_blur.csv', 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(tuple(row.values()))
        csvfile.close()
def start_url_to_text(input_file,output_file,num_threads):
    global nick_data
    global post_output_file
    post_output_file=output_file
    nick_data = get_nick_name_data()
    input_file=pd.read_csv(input_file,index_col=False).fillna('')
    working_input = meta_work(input_file)
    #
    time.sleep(1)
    print(working_input)
    # sys.exit()
    # # list_of_lists = working_input.values.tolist()
    list_of_dicts = working_input.to_dict('records')
    execution_id = str(uuid.uuid4())
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(lambda row: process_url(row, execution_id), list_of_dicts)