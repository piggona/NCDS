import requests
import time
import random
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# 禁用安全请求警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class InterfaceCorrelation:
    def __init__(self):
        now = str(int(time.time()))
        self.ip = "https://api.18sjkj.com"
        app_version = "3.1.1"
        token = "OEEwQzBBMEQtMDc1Qi00OURELTk1MTEtODNDN0JFMjFFNTA0MGViZTZiZWU5NWM1MWQwNDU0ZmFhNjM1OGZjODIyZGYxNTQ4Nzg5NDk0"
        self.header = {
                    "res": "640*1136",
                    "pkg": "com.digitalplus.lending",
                    "channel": "app store",
                    "os": "ios",
                    "os-version": "8.0.2",
                    'x-token': token,
                    "device-model": "iPhone1",
                    "app-version": app_version,
                    "conn": '1',
                    "carrier": '1',
                    "device-id": "8A0C0A0D-075B-49DD-9511-83C7BE21E504",
                    "rt": now,
                    "la": "zh-cn",
                    "User-Agent": "QKL/2.0.0 (iPhone; iOS 11.4.1; Scale/3.00)"
                }
        self.scene_id = "1007"

    def myInfor(self):
        '''
        新闻列表接口
        :return: article/trace_info/trace_id
        '''
        data = {
            "type": "1",
            "refresh": "1",
            "tab": self.scene_id
        }
        tab_api = self.ip + "/myInfor"
        r = requests.get(url=tab_api, params=data, headers=self.header, verify=False)
        response = r.json()
        news_list = response["data"]["list"]
        news_num = len(news_list)
        print(news_num)
        a = {}
        b = []
        for news in news_list:
            try:
                article_id = news["article_id"]
            except BaseException as e:
                article_id = None
            if article_id:
                a['article_id'] = news["article_id"]
                a['trace_info'] = news["trace_info"]
                a['trace_id'] = news["trace_id"]
                b.append(a)
        print(b)
        return b

    def infor_expose(self, b):
        """刷新后曝光4条新闻"""
        for i in range(4):
            article_id = b[i]['article_id']
            trace_info = b[i]["trace_info"]
            trace_id = b[i]["trace_id"]
            api = "/infor/myFeedback?type={expose}"   # "type": "",  # expose  , click,  stay
            url = self.ip + api
            data= {
                "item_id": article_id,  # 行为类型         article_id
                "trace_info": trace_info,    # 回传trace_info
                "trace_id": trace_id,      # 回传 trace_id
                "item_type": "article",     # 内容类型
                "bhv_type": "expose",    # 行为类型 如点击，曝光
                "bhv_value": "1",   # 行为详情 如点击次数详情  int (树枝)   停留随机20-30， 点击/曝光都是1
                "scene_id": self.scene_id     # 频道ID
            }
            req = requests.post(url=url, data=data, headers=self.header, verify=False)
        print("曝光成功", req.json())

    def infor_click(self, article_id,trace_info,trace_id):
        """点击新闻"""
        api = "/infor/myFeedback?type={click}"  # "type": "",  # expose  , click,  stay
        url = self.ip + api
        data = {
            "item_id": article_id,  # 行为类型         article_id
            "trace_info": trace_info,  # 回传trace_info
            "trace_id": trace_id,  # 回传 trace_id
            "item_type": "article",  # 内容类型
            "bhv_type": "click",  # 行为类型 如点击，曝光
            "bhv_value": "1",  # 行为详情 如点击次数详情  int (树枝)   停留随机20-30， 点击/曝光都是1
            "scene_id": self.scene_id  # 频道ID
        }
        req = requests.post(url=url, data=data, headers=self.header, verify=False)
        print("点击完成", req.json())

    def infor_stay(self,article_id,trace_info,trace_id):
        """停留"""
        random_num = random.randint(20, 30)
        api = "/infor/myFeedback?type={stay}"  # "type": "",  # expose  , click,  stay
        url = self.ip + api
        data = {
            "item_id": article_id,  # 行为类型         article_id
            "trace_info": trace_info,  # 回传trace_info
            "trace_id": trace_id,  # 回传 trace_id
            "item_type": "article",  # 内容类型
            "bhv_type": "stay",  # 行为类型 如点击，曝光
            "bhv_value": random_num,  # 行为详情 如点击次数详情  int (树枝)   停留随机20-30， 点击/曝光都是1
            "scene_id": self.scene_id  # 频道ID
        }
        req = requests.post(url=url, data=data, headers=self.header, verify=False)
        print("停留完成%s " %req.json())


    def click_stay(self, article_id, trace_info, trace_id):
        """点击+停留"""
        self.infor_click(article_id, trace_info, trace_id)
        self.infor_stay(article_id, trace_info, trace_id)
        print("点击+停留执行完成")


if __name__ == '__main__':
    interface = InterfaceCorrelation()
    b = interface.myInfor()
    interface.infor_expose(b)
    r_num = random.randint(0, 9)
    article_id = b[r_num]['article_id']
    trace_info = b[r_num]["trace_info"]
    trace_id = b[r_num]["trace_id"]
    interface.click_stay(article_id, trace_info, trace_id)
