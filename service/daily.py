#!/usr/bin/env python
# -*- coding: utf8 -*-

import os,sys
import tushare as ts
import numpy as np
import json
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer

prod = KafkaProducer(bootstrap_servers=['192.168.182.129:9092'])

def map_func(code):
    stock_daily_result_array =b''
    ts.set_token(u"97728e6a2de1a54697c1ce140f924990c2b966d01ca26b925ee10441")
    pro = ts.pro_api(timeout=30)
    try:
        #获取当日行情
        res = pro.query(api_name=bytes("daily",encoding="utf-8"),ts_code=bytes(code,encoding="utf-8"), start_date=bytes("20050101",encoding="utf-8"), end_date=bytes("20201231",encoding="utf-8"))
        # #获取某个股票的日行情数据
        stock_daily_results = res.to_json(orient="records")
        stocks = json.loads(stock_daily_results)
        for stock in stocks:
            print(type(json.dumps(stock)))
            prod.send(topic='stock',value=json.dumps(stock).encode())
    except Exception as e:
        print(code,e.args)


if __name__ == "__main__":
    ts.set_token("97728e6a2de1a54697c1ce140f924990c2b966d01ca26b925ee10441")
    pro = ts.pro_api(timeout=30)

    #获取当前所有正常上市交易的股票列表
    stocks = pro.query('stock_basic',exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    stock_array = np.array(stocks['ts_code']).tolist()

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(map_func,stock_array[0:500])
