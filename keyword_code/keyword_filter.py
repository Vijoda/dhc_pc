import pandas as pd
from keyword_code.keyword_data import read_google_sheet_with_url
import Setting_files.settings as settings
from urllib.parse import urlparse


def keyword_filter(data_to_list,keyword_data):
    negetive_url_list = []
    negetive_url_dict_list=[]
    negetive_data_list=keyword_data['Negative Keywords'].tolist()+keyword_data['Education Keywords'].tolist()+keyword_data['News Keywords'].tolist()
    negative_data_list = [item for item in negetive_data_list if item != '']
    count=0
    for url in data_to_list:
        count=count+1
        print(count)
        # print(url)
        if 'http' not in url:
            url_p='https://'+url
        else:url_p=url
        # print(url_p)
        # print(urlparse(url_p).path)
        parsed_path = urlparse(url_p).path
        for k in negative_data_list:
            if str(k) in parsed_path:
                negetive_url_dict_list.append({'url':url,'n_keyword':k})
                negetive_url_list.append(url)
                pass
    return negetive_url_dict_list,negetive_url_list


# keyword_data = read_google_sheet_with_url(settings.GOOGLE_KEYWORD_SHEET)
# data_to_test=pd.read_csv('google_direct_input.csv')
# data_to_list=data_to_test['qpkey'].to_list()
# negetive_url_dict_list,negetive_url_list=keyword_filter(data_to_list,keyword_data)

