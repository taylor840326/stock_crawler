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

#本程序用于定时获取上市的股票信息，把股票信息保存到Kafka后，再在本地保存一份数据。
if __name__ == "__main__":

    #读取配置文件
    conf = configparser.ConfigParser()
    conf.read("config.ini", encoding="utf-8")
    kafka_bootstrap_servers = conf.get("kafka", "bootstrap_servers")
    kafka_topic = conf.get("kafka", "stock_list_topic")

    #登录tushare接口
    ts.set_token(conf.get("tushare", "token"))
    pro = ts.pro_api(timeout=30)

    # 初始化Kafka生产者
    # prod = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers,
                        #  compression_type='lz4', acks=1, retries=3)


    # 获取当前所有正常上市交易的股票列表
    stocks = pro.query('stock_basic', exchange='', list_status='L',
                           fields='ts_code,symbol,name,area,industry,list_date')
    stock_json = json.loads(stocks.to_json(orient="records"))

    #发送数据到Kafka
    # prod.send(topic=kafka_topic, value=json.dumps(stock_json).encode())



