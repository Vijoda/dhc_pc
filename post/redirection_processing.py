import pandas as  pd
import psycopg2
import Setting_files.settings as settings
from os.path  import exists
import csv
import uuid
import time
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.common.proxy import Proxy, ProxyType
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
def meta_work(input_file):
    if not exists('meta_redirection.csv'):
        with open('meta_redirection.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(list(input_file.columns))
            csvfile.close()

    meta_file = pd.read_csv('meta_redirection.csv', index_col=False).fillna('')
    # working_input=input_file.merge(meta_file,how='outer')
    working_input =pd.concat([input_file,meta_file]).drop_duplicates(keep=False)

    working_input=working_input[working_input['output_domain']=='']

    return working_input
def database_connect():
    db_params = DATABASE_CREDENTIALS
    try:
        connection = psycopg2.connect(**db_params)
        return connection
    except Exception as ex:
        print(ex)
        return None
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

def update_redirection_table(input_domain, output_domain, table_name):
    connection = database_connect()
    try:
        query = f'''
            UPDATE "{table_name}"
            SET output_domain = %s
            WHERE input_domain = %s
            '''
        cursor = connection.cursor()
        cursor.execute(query, (output_domain, input_domain))
        connection.commit()
        cursor.close()
    except Exception as e:
        print(f"An error occurred: {e}")
        # Rollback in case of error
        connection.rollback()
    finally:
        connection.close()
def launch_browser(url):
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
    time.sleep(1)

    output_url = browser.current_url
    browser.close()
    return output_url
def process_url(row, execution_id):
    url = row[0]
    print(url)
    if 'http' not in url:
        working_url='http://'+url
    else:working_url =url
    output_url=launch_browser(working_url)
    output_domain=urlparse(output_url).netloc
    output_domain=output_domain.replace('www.','')
    update_redirection_table(url, output_domain, settings.DHCP_REDIRECTION_FILE)
def start_redirection(num_threads):
    input_file=get_redirection_output_file()
    print(input_file)
    input_file = input_file[input_file['output_domain'] == '']
    print(input_file)
    # list_of_dicts = working_input.to_dict('records')
    list_of_lists = input_file.values.tolist()
    # print(list_of_dicts)
    execution_id = str(uuid.uuid4())
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(lambda row: process_url(row, execution_id), list_of_lists)