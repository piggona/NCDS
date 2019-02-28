# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import requests
import random
import time
import re

from baidu_article_spider.utils.get_proxy import *

now = str(int(time.time()))


def write(path, text):
    with open(path, 'a', encoding='utf-8') as f:
        f.writelines(text)
        f.write('\n')

def re_str(number):
    """
    随机一个生成一个字符串
    :param number: 传入一个整整数
    :return: 返回一个对应的number的字符串
    """
    a = "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    number = int(number)
    sa = []
    for i in range(number):
        sa.append(random.choice(a))
    salt = ''.join(sa)
    # print(salt)
    return salt


def get_imei(device):
    imei = None
    if device == "ios":
        imei = "%s-%s-%s-%s-%s" % (re_str(8), re_str(4),
                                   re_str(4), re_str(4), re_str(12))
    else:
        ra = random.randint(100000000000000, 999999999999999)
        imei = "aimei" + str(ra)
    return imei


def get_mac():
    mac = "%s:%s:%s:%s:%s:%s" % (re_str(2), re_str(
        2), re_str(2), re_str(2), re_str(2), re_str(2))
    return mac


def get_pkg(device):
    if device == "ios":
        pkg = "om.fkkc.shujiakeji"
    else:
        pkg = "com.digitalplus.lending"
    return pkg


def get_k_v(iphone):
    '''
    传入一个字典，随机返回k 和v
    :param iphone: 字典
    :return: k，v
    '''
    keys = []
    for key in iphone:
        keys.append(key)
    num = len(keys)
    number = random.randint(0, num-1)
    k = keys[number]
    v = iphone[k]
    return k, v


def get_res_os(device):
    if device == "ios":
        iphone = {
            "iPhone 6": "750*1334",
            "iPhone 6 Plus": "1080*1920",
            "iPhone 6S": "750*1334",
            "iPhone 6S Plus": "1080*1920",
            "iPhone SE": "640*1136",
            "iPhone 7": "750*1334",
            "iPhone 7 Plus": "1080*1920",
            "iPhone X": "1242*2800",
        }
        device_model, res = get_k_v(iphone)
    else:
        android = {
            "MHA-ALOO": {"vendor":"HUAWEI","screenWidth":1080,"screenHeight":1808},
            "EVR-AL00": {"vendor":"HUAWEI","screenWidth":1080,"screenHeight":2244},
            "vivo X21A": {"vendor":"VIVO","screenWidth":1080,"screenHeight":2280},
            "vivo Y85A": {"vendor":"HUAWEI","screenWidth":1080,"screenHeight":1808},
            "MIX 2": {"vendor":"XIAOMI","screenWidth":1080,"screenHeight":2160},
            "MI 6": {"vendor":"XIAOMI","screenWidth":1080,"screenHeight":1920},
            "vivo X20A": {"vendor":"VIVO","screenWidth":1080,"screenHeight":2160},
            "Redmi Note 4X": {"vendor":"XIAOMI","screenWidth":1080,"screenHeight":1920},
            "HTC 2Q4R400": {"vendor":"HTC","screenWidth":1440,"screenHeight":2880},
            "SM-N9006": {"vendor":"SAMSUNG","screenWidth":1080,"screenHeight":1920},
            "MX5": {"vendor":"MEIZU","screenWidth":1080,"screenHeight":1920},
            "SM-N9009": {"vendor":"SAMSUNG","screenWidth":1080,"screenHeight":1920},
            "R8207":  {"vendor":"OPPO","screenWidth":720,"screenHeight":1280},
            "MI 8": {"vendor":"XIAOMI","screenWidth":1080,"screenHeight":2248},
            "V1816A": {"vendor":"VIVO","screenWidth":1080,"screenHeight":2340},
            "ONEPLUS A6000": {"vendor":"ONEPLUS","screenWidth":1080,"screenHeight":2280},
        }
        device_model, res = get_k_v(android)
    return device_model,res


def get_os_version(device):
    if device == "ios":
        ios = ["11.2", "11.2.1", "11.2.2", "11.1.2", "11.1"]
        os_version = random.choice(ios)
    else:
        android = ["8.0.2", "8.0.0", "7.1", "7.0", "6.0"]
        os_version = random.choice(android)
    return os_version


def ra_header(device, token):
    '''
    获取一个模拟的header
    input: device(设备类型：ios/Android),token(用户获取的token信息)
    output: 完整的header
    '''
    res, device_model = get_res_os(device)
    headers = {
        "res": res,
        "pkg": get_pkg(device),
        "channel": "shujia_haohao",
        "os": device,
        "os-version": get_os_version(device),
        'x-token': token,
        "device-model": device_model,
        "app-version": "3.1.1",
        "conn": '1',
        "carrier": '1',
        "device-id": get_imei(device),
        "rt": now,
        "la": "zh-cn",
        "User-Agent": "QKL/2.0.0 (iPhone; iOS 11.4.1; Scale/3.00)",
        "mac": get_mac()
    }
    return headers


def get_device():
    device = {"imei":get_imei("android"),"mac":get_mac(),"androidId":"Android"+str(random.randint(0,10000)),"model":"","vendor":"","screenWidth":"","screenHeight":"","osType":1,"osVersion":get_os_version("android"),"deviceType":1}
    device_model,info = get_res_os("android")
    print(info)
    device["model"] = device_model
    device["vendor"] = info["vendor"]
    device["screenWidth"] = info["screenWidth"]
    device["screenHeight"] = info["screenHeight"]
    return device

def get_network():
    pattern = re.compile(".*?:")
    proxy = get_proxy()
    ipv4 = pattern.match(proxy)
    print(ipv4)
    network = {"ipv4":ipv4.group(0)[:-1],"connectionType":100,"operatorType":99}
    return network

def get_contentParams(page,catIds):
    contentParams = {"pageSize":20,"pageIndex":page,"adCount":0,"catIds":catIds,"contentType":0}
    return contentParams

def get_request(requestId):
    catIds = [1001,1002,1005,1006,1007,1008,1009,1011,1012,1013,1015,1016,1017,1018,1019,1020,1021,1026,1029,1027,1031]
    request = {"requestId":requestId,"appsid":"ffd2bb8b","device":get_device(),"Network":get_network(),"contentParams":get_contentParams(0,catIds)}
    print(request)
    return request