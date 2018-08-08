# -*- coding: utf-8 -*-

import datetime
from concurrent.futures import ThreadPoolExecutor
import requests
from flask import Flask
from flask import request
import CreditLife_Process.ff_online_production_var_generate as ff_process

executor = ThreadPoolExecutor(10)


def log_cur_time(in_str=None):
    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 现在
    print(nowTime + " " + str(in_str))


def process_request_and_callback(in_body_json, in_callback_url):
    # feature framework processing here
    dic_result = ff_process.ff_online_process(in_body_json)
    log_cur_time(dic_result)

    try:
        # call back Credit System Var to send Generation results
        response = requests.get(in_callback_url, data=dic_result.to_dict())
        # print(response.content.decode())
        # log_cur_time(in_body_json)
        log_cur_time("Callback status: " + in_callback_url + " : " + response.status_code)
    except Exception as e:
        print("Exception calling callback URL: ", in_callback_url)
        print(e)

app = Flask(__name__)


@app.route('/', methods=['POST'])
def index():
    if request.method == 'POST':
        body_json = request.get_json(force=True)
    executor.submit(process_request_and_callback, in_body_json=body_json, in_callback_url="https://www.baidu.com")
    return "Hello, World!"


if __name__ == '__main__':
    app.run(debug=True)
    # app.run()
