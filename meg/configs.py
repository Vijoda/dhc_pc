import os
from urllib.parse import urljoin

# BASE_API_END_POINT = os.environ['BASE_API_END_POINT']
# MUSER = os.environ['MUSER']
# MPASSWORD = os.environ['MPASSWORD']
BASE_API_END_POINT = "http://herculesv1.datables.ai/"
MUSER = "vijay.gupta@forage.ai"
MPASSWORD = "GyxPWoZqsigSq7YvfLGJFc"


LOGIN_API_END_POINT = urljoin(BASE_API_END_POINT, "login")
DASHBOARD_END_POINT = urljoin(BASE_API_END_POINT, "ucdash")
# UPLOAD_SNAP_END_POINT = urljoin(BASE_API_END_POINT,'get_requeue_data')
REQ_REPORT_END_POINT = urljoin(BASE_API_END_POINT, "request_report")
REQ_GREPORT_END_POINT = urljoin(BASE_API_END_POINT, "request_greport")
PUBLIC_REPORT_LINK = urljoin(BASE_API_END_POINT, "report_status")
UPLOAD_STATUS_CHECK = urljoin(BASE_API_END_POINT, "up_status")
REQUEUE_STATUS_ENDPOINT = urljoin(BASE_API_END_POINT, "requeue")
