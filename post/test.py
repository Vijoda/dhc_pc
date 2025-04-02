#
# from playwright.sync_api import sync_playwright
# import time
# dict_proxy = {"server": "https://network.joinmassive.com:65535",
#               "username": "weboliath@gmail.com",
#               "password": "56j5NlvUnc95hlWsxIUPR8fSJfCzuTOr9TqKhSbq"}
# with sync_playwright() as p:
#     browser = p.chromium.launch(headless=False,proxy=dict_proxy )
#     # browser = p.chromium.launch(headless=False)
#
#     context = browser.new_context()
#     page = context.new_page()
#     page.goto('https://example.com/',wait_until='load',timeout=600000)
#     output_url = page.url
#     html = page.content()
#     time.sleep(2)
#     context.close()
#     browser.close()


import pandas as pd

daat=pd.read_csv('QaGEjK8jc5htsjqK33xRrq_OG.csv',index_col=False).fillna('')
daat['credential']='MD'
daat.to_csv('ddd.csv',index=False)