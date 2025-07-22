import requests
import json


class EmailParam:

    def __init__(self, param: dict):
        self.param = {}
        self.param.setdefault("MOType", param.get("MOType", "APM"))
        self.param.setdefault("MOName", param.get("MOName", "APM"))
        self.param.setdefault("AlertName", param.get("AlertName", "Email"))
        self.param.setdefault("AlertDescription", param.get("AlertDescription", "Empty"))
        self.param.setdefault("Priority", param.get("Priority", "P4"))
        self.param.setdefault("EventID", param.get("EventID", "5264137"))
        self.param.setdefault("Assignee", param.get("Assignee", "longyu3"))

    def get_param(self):
        return self.param

# 发送邮件类


class Email:

    url = "https://metis.imonitoring.lenovo.com/raw_event/add"

    # 初始化参数
    def __init__(self, param: dict, token: str):
        self.param = {}
        self.param.setdefault("rawdata", param)
        self.param.setdefault("token", token)
        self.token = token

    # 发送email
    def send(self) -> str:
        fails = 0
        while True:
            try:
                if fails >= 2:
                    break
                headers = {
                    # 'content-type': 'application/json',
                    # 'apiKey': self.token
                }
                ret = requests.post(self.url, json=self.param, headers=headers, timeout=10, verify=False)
                if ret.status_code == 200:
                    text = ret.text
                else:
                    continue
            except Exception:
                fails += 1
                print('网络连接出现问题, 正在尝试再次请求: ', fails)
            else:
                break
        return text

# use case
# Email(EmailParam({"AlertDescription": "这是一封测试邮件",  "AlertName": "Test"}).get_param(),"105b8a825edb4db3a427e0f5092ce5c6").send()