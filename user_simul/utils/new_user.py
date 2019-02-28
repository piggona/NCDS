# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：Yan,Haohao


import os
import requests

from user_simul.utils.get_header import *
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# 禁用安全请求警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

ip = "https://api.18sjkj.com"
youke = "/user/tourist_login"

def new_user(device):
    '''
    获取一个新用户，主要获取user_id及token
    input:
    output: user
    '''
    url = ip + youke
    data = {'umeng': "1"}
    header = ra_header(device,"")
    req = requests.post(url=url, data=data, headers=header,verify=False)
    token = req.json()
    return token

if __name__ == "__main__":
    result = new_user()
    print(result)
