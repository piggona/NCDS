# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：Yan,Haohao

import random
import time

now = str(int(time.time()))


def write(path, text):
    with open(path, 'a', encoding='utf-8') as f:
        f.writelines(text)
        f.write('\n')


def ra_imei():
    '''
    随机生成imei值
    '''
    sum = 0
    while True:
        ra = random.randint(100000000000000, 999999999999999)
        a = "aimei" + str(ra)
        write("imei.txt", a)
        sum += 1
        if sum == 100:
            break


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
            "test_iPhone 6": "750*1334",
            "test_iPhone 6 Plus": "1080*1920",
            "test_iPhone 6S": "750*1334",
            "test_iPhone 6S Plus": "1080*1920",
            "test_iPhone SE": "640*1136",
            "test_iPhone 7": "750*1334",
            "test_iPhone 7 Plus": "1080*1920",
            "test_iPhone X": "1242*2800",
        }
        device_model, res = get_k_v(iphone)
    else:
        android = {
            "test_MHA-ALOO": "1080*1808",
            "test_EVR-AL00": "1080*2244",
            "test_vivo X21A": "2280*1080",
            "test_vivo Y85A": "2280*1080",
            "test_MIX 2": "2160*1080",
            "test_MI 6": "1080*1920",
            "test_vivo X20A": "2160*1080",
            "test_Redmi Note 4X": "1080*1920",
            "test_HTC 2Q4R400": "2880*1440",
            "test_SM-N9006": "1080*1920",
            "test_MX5": "1080*1920",
            "test_SM-N9009": "1080*1920",
            "test_R8207": "720*1280",
            "test_MI 8": "2248*1080",
            "test_V1816A": "1080*2340",
            "test_ONEPLUS A6000": "2280*1080",
        }
        device_model, res = get_k_v(iphone=android)
    return res, device_model


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
        "channel": "shujia_test_haohao",
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


if __name__ == '__main__':
    result = ra_header(
        "ios", "OEEwQzBBMEQtMDc1Qi00OURELTk1MTEtODNDN0JFMjFFNTA0MGViZTZiZWU5NWM1MWQwNDU0ZmFhNjM1OGZjODIyZGYxNTQ4Nzg5NDk0")
    print(result)
