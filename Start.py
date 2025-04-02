import time
import  pandas as pd
import numpy as np
# from meg.mactions import get_status
from pre.DHFC_Proccesing import Dhcp
import pre.uc_output_processing_google_input_creation as gi
import pre.google_search_uc_data_processing as gs
import pre.google_search_output_processing as go
import post.Google_Direct_Search_addon as gadd
# from pre.PC_heartbeat import start_auto_retry,check_status
import post.DHCP_url_to_text_mongo as ul
import Setting_files.settings as settings
import post.postprocess_input_creation as po_input
import post.redirection_processing as redirection_processing
import post.Google_Searches as pgs
import post.Blur_Google_Searches as bgs
import post.comuflage as cf
import post.add_keyword_to_final as addkey

def Dhcp_start():

    output_datafraem = Dhcp().dhcp_processing()
     # Dhcp().dhcp_input_analysis()

    upload_id=Dhcp().file_upload()

    ##########get status##############

    status=None
    # while status != 'COMPLETE':
    #     try:
    #         status_data = get_status(upload_id)
    #         status=status_data.get('status')
    #         print(status)
    #         if status=='COMPLETE':
    #             break
    #         time.sleep(5)
    #     except Exception as ex:
    #         print(ex)

    ###################################
    ######## retry ###########

    # max_retry_attempts=1
    # retry_minutes_interval=20
    # requeue_worker_type='NOVPN_UH'
    # stat = start_auto_retry(upload_id,max_retry_attempts,retry_minutes_interval,requeue_worker_type)
    # status = None
    # while status != 'Completed': #and stat.get('status')!='NOT IN DB':
    #     try:
    #         status = check_status(uc_upload_id=upload_id)
    #         print(status)
    #         time.sleep(20)
    #     except Exception as ex:
    #         print(ex)
    # return upload_id
def uc_google_funcation(upload_id=None):
    print('In Google start')
    table_name= gi.start_download_and_process(upload_id)
    output_frame = gi.create_output_frame()
    gi.output_process(upload_id + '.csv', output_frame,table_name)
    gi.save_output_main_file()
    gi.redirection_creation()
    # gi.updating_redirection_file()
    gi.google_input_creation()
    upload_id=gi.file_upload()
    print(upload_id)
    status = None
    # while status != 'COMPLETE':
    #     try:
    #         status_data = get_status(upload_id)
    #         # print(status_data)
    #         status = status_data.get('status')
    #         print(status)
    #         if status=='COMPLETE':
    #             break
    #         time.sleep(12)
    #     except Exception as ex:
    #         print(ex)
    # return upload_id
def google_direct_input(upload_id=None):
    print('In Google Direct')
    gs.start_download_and_process(upload_id)
    gs.input_creation(upload_id)
    time.sleep(3)
    upload_id=gs.file_upload()
    status=None
    # while status != 'COMPLETE':
    #     try:
    #         status_data = get_status(upload_id)
    #         status = status_data.get('status')
    #         print(status)
    #         if status=='COMPLETE':
    #             break
    #         time.sleep(20)
    #     except Exception as ex:
    #         print(ex)
    #
    # ########
    #
    # max_retry_attempts=1
    # retry_minutes_interval=360
    # requeue_worker_type='NOVPN_UH'
    # stat=start_auto_retry(upload_id,max_retry_attempts,retry_minutes_interval,requeue_worker_type)
    # time.sleep(2)
    # status=None
    # while status!='Completed': #and stat.get('status')!='NOT IN DB':
    #     try:
    #         status=check_status(uc_upload_id=upload_id)
    #         print(status)
    #         time.sleep(60)
    #     except Exception as ex:
    #         print(ex)
    # return upload_id
def google_direct_output(upload_id=None):
    table_name=go.start_download_and_process(upload_id)
    table_name=upload_id+'.csv'
    go.output_processing(table_name,upload_id)
    go.updating_redirection_file()
    keyword_file_from_pre_processing=go.output_processing_keyword()
    keyword_file_from_pre_processing=pd.read_csv('Dhcp_processed_output_after_google_uc.csv',index_col=False).fillna('')
    go.filter_and_save_post_processing_data(keyword_file_from_pre_processing)

def url_to_text(input_file,output_file,threads):
    print('In Url to Text')
    input_file=pd.read_csv(input_file,index_col=False,dtype=str).fillna('')
    # proxy_server = "https://d1d3dfa7dc4444a88a253a0263be5877:@proxy.zyte.com:8011"
    ul.start_url_to_text(input_file,output_file,threads)

def googlesearch(input_file,output_file,threads):
    pgs.creating_input(settings.DHCP_GOOGLE_INPUT_UC,settings.DHCP_GOOGLE_FILTER_OUTPUT)
    pgs.start_google_search(input_file,output_file,threads)
    pgs.filter_output(settings.DHCP_GOOGLE_FILTER_OUTPUT)
#
def blur_google_search(input_file,output_file,threads):
    bgs.creating_input(settings.DHCP_GOOGLE_INPUT_UC,settings.DHCP_GOOGLE_FILTER_OUTPUT)
    # bgs.start_url_to_text(input_file, output_file, threads)
    # bgs.filter_output(settings.DHCP_GOOGLE_FILTER_OUTPUT)
#
def camuflage(input_file,output_file,threads):
    # cf.creating_input(settings.DHCP_UC_UPLOAD_FILE_NAME,settings.DHCP_GOOGLE_FILTER_OUTPUT)
    input_file = pd.read_csv(input_file, index_col=False).fillna('')
    cf.start_camuflage(input_file, output_file, threads)
#
def postprocess_input(pre_output_file,dhcp_uc_input):
    print('In Post Input Creation')
    output_file=po_input.get_output_file(pre_output_file)
    dhcp_uc_input = pd.read_csv(dhcp_uc_input, index_col=False).fillna('')
    po_input.input_create(output_file, dhcp_uc_input)

def google_direct_search_addon(input_file,output_file):
#     print('in addon')
#     input_file=pd.read_csv(input_file+'.csv',index_col=False).fillna('')
#     # input_file = pd.read_csv('h' + '.csv', index_col=False).fillna('')
    output_file = pd.read_csv(output_file + '.csv', index_col=False).fillna('')
    # gadd.creating_input(input_file,output_file)
    gadd.start_addon_search(settings.DHCP_Google_direct_addon_input, output_file, 1)
# #     # keyword_file_from_pre_processing = gadd.output_processing_keyword()
# #     # gadd.filter_and_save_post_processing_data(keyword_file_from_pre_processing)
#     gadd.filter_output(settings.DHCP_GOOGLE_FILTER_OUTPUT)
def start_funcation():
    #### get upload direct urls to uc and gives upload id
    # uc_direct_upload_id = Dhcp_start()
    # #### get upload direct upload_id and create google input and upload it
    # uc_google_upload_id = uc_google_funcation(uc_direct_upload_id)
    # ##### get upload_id for google output and create direct_url from it
    # uc_google_direct_upload_id = google_direct_input(uc_google_upload_id)
    # ##### process uc google output and comabine that with uc direct output
    # google_direct_output(uc_google_direct_upload_id)
    # #google_addon_search
    ## google_direct_search_addon(uc_google_direct_upload_id, settings.DHCP_GOOGLE_FILTER_OUTPUT)
    # # postprocessing
    # postprocess_input(settings.DHCP_GOOGLE_FILTER_OUTPUT,settings.DHCP_UC_UPLOAD_FILE_NAME)
    # # Post postprocessing URL to text
    # url_to_text('post_input.csv',settings.DHCP_GOOGLE_FILTER_OUTPUT,9)
    # # redirection updaing
    # redirection_processing.start_redirection(7)
    # #Google search
    # googlesearch(settings.DHCP_Post_Google_search_input,settings.DHCP_GOOGLE_FILTER_OUTPUT,15)
    # # Google search blur
    # blur_google_search(settings.DHCP_Post_Blur_Google_search_input,settings.DHCP_GOOGLE_FILTER_OUTPUT, 6)
    #
    camuflage(settings.DHCP_Post_Comuflage_input,settings.DHCP_GOOGLE_FILTER_OUTPUT,10)
    #
    addkey.keyword_adding()

if __name__ == '__main__':
    start_funcation()