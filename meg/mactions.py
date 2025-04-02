from .msession import init_session
from .configs import (DASHBOARD_END_POINT, REQUEUE_STATUS_ENDPOINT,UPLOAD_STATUS_CHECK)
import logging,sys,json, time

# logging
formatter = logging.Formatter(fmt='%(asctime)s %(name)s %(process)d %(levelname)-8s %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S')
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(handler)

logger.setLevel(logging.DEBUG)

# end logging
session, csrf_token = init_session()

def upload_file(file_path, upload_type, upload_vpn='None',priority= False):
    global session,csrf_token
    session,csrf_token =  init_session()
    files = {'filename': open(file_path, 'rb')}
    payload = dict()
    payload['ptype'] = upload_type
    payload['vtype'] = upload_vpn
    payload['submit'] = 'Upload'
    payload['csrf_token'] = csrf_token
    payload['priority'] = priority
    payload['utype'] = 'bot'
    
    dict_rupload = session.post(DASHBOARD_END_POINT, files=files,data=payload)
    dict_data = dict()    
    if dict_rupload.status_code == 200:
        try:
            dict_data = dict_rupload.json()
            logger.info(dict_data)
        except Exception as e:
            logger.exception(e)
            logger.info(dict_rupload.text)
    else:
        dict_data = {'msg':'something wrong happened','stauts':'fail'} 
    if 'csrf_token' in json.dumps(dict_data):
        logger.info('regen csrf')
        session,csrf_token =  init_session()
        dict_data['csrf_reval'] = 'csrf token re-validated please try again'

    logger.info('wait for file success upload')
    upload_id = dict_data['upload_id']
    cstatus = get_status(upload_id=upload_id)
    while cstatus['status'] not in ['INPROGRESS','UPLOADED','COMPLETE']:
        logger.info('file still in progress current status %s',cstatus)
        cstatus = get_status(upload_id=upload_id)
        time.sleep(10) 
    return upload_id


def get_status(upload_id):
    dict_data = {'upload_id':upload_id}
    r = session.post(UPLOAD_STATUS_CHECK,json=dict_data)
    ustatus = None
    if r.status_code == 200:
        ustatus = r.json()
    else:
        ustatus = 'APIERROR'
    return ustatus

#def request_download(upload_id):
#    """
#        request the download for the file 
#    """
#    dict_dt = {'upload_id':upload_id}
#    download_id = None
#    rs  = session.post(REQ_REPORT_END_POINT,json=dict_dt)
#    dict_data = dict()
#
#    if rs.status_code == 200:
#        dict_data['status'] = 'success'
#        dict_data['download_id'] = rs.text
#    else:
#        dict_data['status'] = 'fail'
#    
#    return dict_data 
#
#def download_file(report_id):
#    """
#        returns public s3 link of file with status , this function can be used for both 
#        getting url and checking status
#    """
#    jdata = {'report_id':report_id}
#    rs = session.post(PUBLIC_REPORT_LINK,json=jdata)
#    return rs.json()
#

def requeue_status(upload_id,status,worker_type='null'):
    """
        re-queue status for specific upload id 
    """
    if not isinstance(status, list):
        raise AttributeError('status should be a list')

    jdata = dict()
    
    jdata['upload_id'] = upload_id
    jdata['status'] = status
    jdata['worker_type'] = worker_type
    
    resp = session.post(REQUEUE_STATUS_ENDPOINT,json=jdata)

    return resp.json()

