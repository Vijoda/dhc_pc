import requests , os
from bs4 import BeautifulSoup
from .configs import LOGIN_API_END_POINT, MUSER,MPASSWORD


def init_session():

    global session,csrf_token
    session = requests.Session()
    rs = session.get(LOGIN_API_END_POINT)
    soup = BeautifulSoup(rs.text,'html.parser')
    csrf_token = soup.find('input',{'id':'csrf_token'})['value']

    login_data = dict()
    login_data['email'] = MUSER
    login_data['password'] = MPASSWORD
    login_data['csrf_token'] = csrf_token
    login_data['Submit'] ='Login'
    login_rs = session.post(LOGIN_API_END_POINT, data=login_data)

    if login_rs.text == 'fail':
        raise ValueError('uc login failed,please check credentials')
    return session,csrf_token
