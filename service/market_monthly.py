#!/usr/bin/env python
# -*- coding: utf8 -*-

import os
import sys
import time
import tushare as ts
import numpy as np
import json
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer
import configparser


def map_func(code, start_date, end_date):
    # 初始化Kafka生产者
    prod = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers,
                         compression_type='lz4', acks=1, retries=3)

    stock_daily_result_array = b''

    # 登录tushare API
    ts.set_token(conf.get("tushare", "token"))
    pro = ts.pro_api(timeout=30)
    try:
        # 获取当日行情
        res = pro.query(api_name=bytes("daily", encoding="utf-8"), ts_code=bytes(code, encoding="utf-8"),
                        start_date=bytes(start_date, encoding="utf-8"), end_date=bytes(end_date, encoding="utf-8"))
        # #获取某个股票的日行情数据
        stock_daily_results = res.to_json(orient="records")
        stocks = json.loads(stock_daily_results)
        for stock in stocks:
            prod.send(topic=kafka_topic, value=json.dumps(stock).encode())
    except Exception as e:
        print(code, e.args)


if __name__ == "__main__":
    # 本程序的参数为一个精确到日的时间戳，从环境变量中读取。
    # 如果没有参数就打印使用方法，然后退出。退出码为1
    trade_start_date = os.getenv("TRADE_START_DATE", "20000101")
    trade_end_date = os.getenv(
        "TRADE_START_DATE", time.strftime("%Y%m%d", time.localtime()))
    trade_code = os.getenv("TRADE_CODE")

    #读取配置文件
    conf = configparser.ConfigParser()
    conf.read("config.ini", encoding="utf-8")
    kafka_bootstrap_servers = conf.get("kafka", "bootstrap_servers")
    kafka_topic = conf.get("kafka", "topic")

    #登录tushare接口
    ts.set_token(conf.get("tushare", "token"))
    pro = ts.pro_api(timeout=30)

    if trade_code is None:
        # 获取当前所有正常上市交易的股票列表
        stocks = pro.query('stock_basic', exchange='', list_status='L',
                           fields='ts_code,symbol,name,area,industry,list_date')
        stock_array = np.array(stocks['ts_code']).tolist()

        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(map_func, stock_array)
    else:
        map_func(trade_code, trade_start_date, trade_end_date)
