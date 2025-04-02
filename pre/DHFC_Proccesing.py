import Setting_files.settings as settings
import pandas as pd
import numpy as np
from meg.mactions import upload_file
import psycopg2
from urllib.parse import urlparse
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

class Dhcp():

    def __init__(self):
        self.DHCP_INPUT_FILE_NAME = settings.DHCP_INPUT_FILE_NAME
        self.DHCP_UC_UPLOAD_FILE_NAME = settings.DHCP_UC_UPLOAD_FILE_NAME
        self.FEEDBACK_TABLE = settings.FEEDBACK_TABLE
        self.ANALYSIS_OUTPUT = settings.DHCP_ANALYSIS_OUTPUT
    def create_direct_uc_input(self,input_file):
        output_dataframe = pd.DataFrame()
        output_dataframe['company_id'] = input_file["HOSPITAL_ID"]
        output_dataframe['contact_id'] = input_file["NPI"]
        output_dataframe['npi_hospital_id'] = input_file["NPI"].astype(str) + input_file["HOSPITAL_ID"].astype(str)
        output_dataframe['first_name'] = input_file["FIRST_NAME"]
        output_dataframe['middle_name'] = input_file["MIDDLE_NAME"]
        output_dataframe['last_name'] = input_file["LAST_NAME"]
        output_dataframe['website'] = input_file['WEBSITE']
        output_dataframe['parent_website'] = input_file['PARENT_WEBSITE']
        output_dataframe['credential'] = input_file['CREDENTIAL']
        output_dataframe['nick_name'] = ''
        output_dataframe['qpkey'] = input_file['WEBSITE']
        output_dataframe['client_name'] = "DHC"
        output_dataframe['client_tag'] = "DHC"
        output_dataframe['do_uc'] = "NOVPN_UH"
        output_dataframe['s3ttl'] = "30"
        output_dataframe['do_lg'] ='No'
        output_dataframe = output_dataframe.dropna(subset=['qpkey'])
        output_dataframe = output_dataframe.fillna('')
        output_dataframe.to_csv(self.DHCP_UC_UPLOAD_FILE_NAME, index=False)
        return output_dataframe
    def dhcp_processing(self):
        input_file = pd.read_csv(self.DHCP_INPUT_FILE_NAME)
        output_dataframe=self.create_direct_uc_input(input_file)
        return output_dataframe
    def file_upload(self):
        upload_id = upload_file(self.DHCP_UC_UPLOAD_FILE_NAME, 'NOVPN_UH')
        print(upload_id)
        return upload_id
    def sql_connection(self):
        try:
            CONNECTION = psycopg2.connect(**DATABASE_CREDENTIALS)
            return CONNECTION
        except:
            CONNECTION=None
            return CONNECTION
    def dhcp_input_analysis(self):

        input_file = pd.read_csv(self.DHCP_INPUT_FILE_NAME)
        FEEDBACK_TABLE=self.FEEDBACK_TABLE

        # Filling all nan and NULL
        input_file = input_file.fillna('')
        input_file = input_file.replace('NULL', '')
        input_file = input_file.replace('null', '')

        # Check for null or Character in IDs
        input_file['Character in NPI'] = input_file['npi'].apply(lambda x: True if x == '' else False)
        input_file['Character in NPI'] = input_file['npi'].apply(lambda x: any(c.isalpha() for c in str(x)))

        # Check for null or Character in IDs
        input_file['Character in Hospital_id'] = input_file['hospital_id'].apply(lambda x: True if x == '' else False)
        input_file['Character in Hospital_id'] = input_file['hospital_id'].apply(
            lambda x: any(c.isalpha() for c in str(x)))

        # Check for numbers in names
        input_file['Number in First Name'] = input_file['first_name'].apply(lambda x: any(c.isdigit() for c in str(x)))
        input_file['Number in Middle Name'] = input_file['middle_name'].apply(
            lambda x: any(c.isdigit() for c in str(x)))
        input_file['Number in Last Name'] = input_file['last_name'].apply(lambda x: any(c.isdigit() for c in str(x)))

        # Check for middle name lenght
        input_file['Middle name length'] = ''
        input_file['Middle name length'] = input_file['middle_name'].apply(
            lambda x: '' if len(x) >= 3 else 'Middle name lenght id less then three')
        input_file['Middle name length'] = np.where(input_file['middle_name'].str.len() == 0, 'Middle name is empty',
                                                    input_file['Middle name length'])

        # Check for Empty in name
        input_file['Empty in name'] = ''
        input_file['Empty in name'] = np.where(input_file['first_name'].str.lower() == '', 'Empty value in First Name',
                                               input_file['Empty in name'])
        input_file['Empty in name'] = np.where(input_file['middle_name'].str.lower() == '',
                                               'Empty value in Middle Name', input_file['Empty in name'])
        input_file['Empty in name'] = np.where(input_file['last_name'].str.lower() == '', 'Empty value in Last Name',
                                               input_file['Empty in name'])

        # Check if http in urls
        input_file['Http not in old_foundinurl'] = input_file['old_foundinurl'].str.contains(r'http', case=False)
        # input_file['Http in feedbackurl'] = input_file['feedbackurl'].str.contains(r'http', case=False)

        input_file['Boolean value in name'] = ''
        input_file['Boolean value in name'] = np.where(input_file['first_name'].str.lower() == 'true',
                                                       'Boolean value in First Name',
                                                       input_file['Boolean value in name'])
        input_file['Boolean value in name'] = np.where(input_file['first_name'].str.lower() == 'false',
                                                       'Boolean value in First Name',
                                                       input_file['Boolean value in name'])
        input_file['Boolean value in name'] = np.where(input_file['middle_name'].str.lower() == 'true',
                                                       'Boolean value in Middle Name',
                                                       input_file['Boolean value in name'])
        input_file['Boolean value in name'] = np.where(input_file['middle_name'].str.lower() == 'false',
                                                       'Boolean value in Middle Name',
                                                       input_file['Boolean value in name'])
        input_file['Boolean value in name'] = np.where(input_file['last_name'].str.lower() == 'true',
                                                       'Boolean value in Last Name',
                                                       input_file['Boolean value in name'])
        input_file['Boolean value in name'] = np.where(input_file['last_name'].str.lower() == 'false',
                                                       'Boolean value in Last Name',
                                                       input_file['Boolean value in name'])

        # # Check feedback Urls

        for table, cloumn in FEEDBACK_TABLE.items():
            CONNECTION=self.sql_connection()
            sql_query = f'SELECT "{cloumn}" FROM "{table}";'
            feedback_urls = pd.read_sql_query(sql_query, CONNECTION)
            CONNECTION.close()
            feedback_urls = feedback_urls.fillna('')
            feedback_urls = feedback_urls.replace('NULL', '')
            feedback_urls = feedback_urls.replace('null', '')
            input_file['FeedbacK_check_' + table] = input_file['feedbackurl'].isin(feedback_urls[cloumn])

        # Get Domain mapped
        input_file['Domain_urls'] = input_file['old_foundinurl'].apply(lambda x: urlparse(x).netloc)
        # input_file['Domain_urls'] = input_file['Domain_urls'].apply(lambda x: x.netloc)
        input_file['Domain_urls'] = input_file['Domain_urls'].str.replace('www.','')
        # Saving Analysised input
        input_file.to_csv(self.ANALYSIS_OUTPUT, index=False)

