# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：Haohao

import requests
import lxml

def get_proxy():
    try:
        response = requests.get('http://127.0.0.1:5010/get')
        print(response.text)
        if response.status_code == 200:
            return response.text
        return None
    except ConnectionError:
        return None

def get_proxy_val():
    p = get_proxy()
    while p is None:
        p = get_proxy()
    proxies = {
        "http" : "http://" + p,
        "https" : "http://{}".format(p)
    }
    return proxies

if __name__ == "__main__":
    pass