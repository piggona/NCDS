import requests

cu_data = {
    "device": {
        "androidId": "TestAndroidId123",
        "deviceType": 1,
        "idfa": "AAAAAAAA-BBBB-CCCC-0000-FFFFFFFFFFFF",
        "imei": "abcdef123456789",
        "mac": "12:34:56:78:90:ab",
        "model": "SM_G7200",
        "osType": 1,
        "osVersion": "4.2.2",
        "screenHeight": 1080,
        "screenWidth": 1920,
        "vendor": "MEIZU",
    },
    "network": {
        "cellular_id": "12345_45678_0",
        "connectionType": 100,
        "ipv4": "192.168.56.1",
        "operatorType": 99,
        #"lon":116.333134,
        #"lat": 40.009545,
        #"mcc":"412",
        #"mnc":"01"
    },
    "contentParams": {
        "pageSize":20,
        "pageIndex":1,
        "adCount":0,
        "catIds": [1001],
        "contentType":0, # 0鏂伴椈锛�1鍥剧墖锛�2瑙嗛
        # "contentTypeInfos":{"dataType":0, "catIds": [1001],},
    },
    "requestId": "2",
    "appsid": "ffd2bb8b",
}


ad_data = {
    "device": {
        "androidId": "TestAndroidId123",
        "deviceType": 1,
        "idfa": "AAAAAAAA-BBBB-CCCC-0000-FFFFFFFFFFFF",
        "imei": "abcdef123456789",
        "mac": "12:34:56:78:90:ab",
        "model": "MX5",
        "osType": 1,
        "osVersion": "4.2.2",
        "screenHeight": 1080,
        "screenWidth": 1920,
        "vendor": "MEIZU"
    },
    "network": {
        "cellular_id": "12345_45678_0",
        "connectionType": 4,
        "ip": "127.0.0.1",
        "operatorType": 2,
        "lon":116.333134,
        "lat": 40.009545,
        "mcc":"412",
        "mnc":"01"
    },
    "requestId": "test_req_11111",
    "channelId": "TXXL001",
}

# ads api. It works following the docs.
# r = requests.post('http://test.ydtad.com:8100/api/test/ads', json=ad_data)


# content api. It returns bad json data.


r = requests.post('http://api.ydtad.com/ydt-server/cu/list', json=cu_data)
print(r.json())
