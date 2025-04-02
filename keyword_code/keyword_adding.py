from keyword_code.keyword_data import read_google_sheet_with_url
from keyword_code.keyword_filter import keyword_filter
import Setting_files.settings as settings
import pandas as pd
import numpy as np
import re

# input_data=pd.read_csv('C:/Projects_all/DHCP_PC_NEW/Proccessed_output_after_google_uc.csv').fillna('')
from urllib.parse import urlparse

def get_path(url_value):
    path_value=''
    path=''
    if url_value!='' and url_value!=None:
        if 'http' in url_value:
            url=url_value
        else:
            url='http://'+url_value
        parsed_url = urlparse(url)
        if parsed_url.path!='':
            path=parsed_url.path
        elif parsed_url.query!='':
            path = parsed_url.query
        elif parsed_url.fragment!='':
            path = parsed_url.fragment
        path_value=path.lower()
        return path_value
    else:
        return path_value

def keyword_find(row, word, col_name):
    # print(888)
    # print(row)
    path = row['path']
    path=str(path)
    path=path.lower()
    col_value = row[col_name]
    if col_value!='' and col_value.lower() in path:
        word=str(word)
        word=word.lower()
        first_index = word[:1]
        last_index = word[-1:]

        if re.match(r"^[a-zA-Z]", first_index):first_word = True
        else:first_word = False
        if re.match(r"^[a-zA-Z]", last_index):last_word = True
        else:last_word = False

        if first_index.isdigit():first_number = True
        else:first_number = False
        if last_index.isdigit():last_number = True
        else:last_number = False

        if first_word and last_word:pattern = re.compile('[^a-zA-Z]' + re.escape(word) + '[^a-zA-Z]|[^a-zA-Z]' + re.escape(word) + '$|^' + re.escape(word) + '[^a-zA-Z]|^' + re.escape(word) + '$')
        elif first_word and not last_word:pattern =re.compile('[^a-zA-Z]' + re.escape(word) + '|^' + re.escape(word))
        elif not first_word and last_word:pattern = re.compile(rf'{re.escape(word)}[^a-zA-Z]|{re.escape(word)}$')
        elif first_number and last_number:pattern = re.compile('[^0-9]' +re.escape(word) + '[^0-9]|^' + re.escape(word) + '[^0-9]|[^0-9]' + re.escape(word) + '$|^' + re.escape(word) + '$')
        elif first_number and not last_number:pattern = re.compile('[^0-9]' + re.escape(word) + '|^' + re.escape(word))
        elif not first_number and last_number:pattern = re.compile( re.escape(word) + '[^0-9]|' + re.escape(word) + '$')
        elif not first_number and not last_number and not first_word and not last_word:pattern = re.escape(word)
        match = re.search(pattern, path)
        if match:
            return word
    else:
        return col_value

def keyword_cloumns_adding(input_data):
    try:
        keyword_data = read_google_sheet_with_url(settings.GOOGLE_KEYWORD_SHEET)
        print(input_data)
        #
        Positive_list=keyword_data['Positive Keywords'].to_list()
        Positive_list = [element for element in Positive_list if element != '']
        #
        Negative_list=keyword_data['Negative Keywords'].to_list()
        Negative_list = [element for element in Negative_list if element != '']
        #
        Education_list = keyword_data['Education Keywords'].to_list()
        Education_list = [element for element in Education_list if element != '']
        #
        News_list = keyword_data['News Keywords'].to_list()
        News_list = [element for element in News_list if element != '']
        #
        input_data['path'] = input_data['Output_URL'].apply(get_path)
        count_1=0
        input_data['Positive_Keywords'] = ''
        input_data['negative_keywords'] = ''
        input_data['education_keywords'] = ''
        input_data['news_keywords'] = ''

        # input_data_no_path=input_data[input_data['path']=='' or input_data['path']=='/' ]
        input_data_no_path = input_data[(input_data['path'] == '') | (input_data['path'] == '/')]

        input_data_with_path=input_data[(input_data['path'] != '') | (input_data['path'] != '/')]
        # input_data=input_data_with_path


        # for po in Positive_list:
        #     input_data_with_path['Positive_Keywords'] = input_data_with_path.apply(keyword_find, args=(po,'Positive_Keywords'),axis=1)
        #     count_1 = count_1 + 1
        #     print(count_1)
        # count_1 = 0
        #
        # for n in Negative_list:
        #     input_data_with_path['negative_keywords'] = input_data_with_path.apply(keyword_find, args=(n,'negative_keywords'),axis=1)
        #     count_1 = count_1 + 1
        #     print(count_1)
        # count_1 = 0
        #
        for e in Education_list:
            input_data_with_path['education_keywords'] = input_data_with_path.apply(keyword_find, args=(e, 'education_keywords'), axis=1)
            count_1 = count_1 + 1
            print(count_1)
        count_1 = 0
        #
        # for p in News_list:
        #     input_data_with_path['news_keywords'] = input_data_with_path.apply(keyword_find, args=(p, 'news_keywords'), axis=1)
        #     count_1 = count_1 + 1
        #     print(count_1)
        input_data_with_path['PDF']=''
        input_data_with_path['PDF'] = np.where(input_data_with_path['path'].str.contains('.pdf'), 'PDF', input_data_with_path['PDF'])

        file=pd.concat([input_data_no_path,input_data_with_path])
        # input_data.drop('path', axis=1, inplace=True)
        file.to_csv('negetive_PostProcessing_uc_keyword.csv', index=False)
        return file
    except Exception as ex:
        print(ex)
        return None

# post_proccesed_output_name = 'Proccessed_output_after_google_uc.csv'
# post_proccesed_output_name = 'negetive_PostProcessing_uc_keyword.csv'
# post_proccesed_output_temp = pd.read_csv(post_proccesed_output_name, index_col=False, on_bad_lines='skip').fillna('')
# post_proccesed_output_temp['negative_keywords']=''
# post_proccesed_output_temp['news_keywords']=''
# post_proccesed_output_temp['education_keywords']=''
# # post_proccesed_output_temp['Positive_Keywords']=''
# keyword_cloumns_adding(post_proccesed_output_temp)
# # keyword_adding(input_data)

# import pandas as pd
# data=pd.read_csv('PostProcessing_uc_keyword.csv',index_col=False).fillna('')
# df = data.drop(columns=['number'])
# df.to_csv('PostProcessing_uc_keyword_19.csv',index=False)
# data['concatenated'] = data['NPI'].astype(str) + data['Hospital_ID'].astype(str)
# print(data['concatenated'].drop_duplicates(keep='first'))