# -*- coding: utf-8 -*-
"""
1. 获取url上数据计算后存储本地
2. 上传本地文件到hdfs
3. 读取hdfs上文件计算后写入hive
"""

import os,sys
import datetime
import logging

from apscheduler.schedulers.blocking import BlockingScheduler

nowPath=os.path.dirname(os.path.abspath(__file__))
homePath=nowPath
sys.path.append(homePath)

import config
appPath=config.appPath
sys.path.append(appPath)

from url2hive import main as url2hive

## 堵塞方式运行计划任务
scheduler = BlockingScheduler()

## 初始化日志
logging.basicConfig(
    ## 日志级别: DEBUG, INFO, WARNNING, ERROR, CRITICAL
    level = logging.INFO,
    ## 日志格式: 时间, 代码所在文件名, 代码行号, 日志级别名字, 日志信息
    format = "%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s",
    ## 日志打印时间
    datefmt = "%m/%d/%Y %H:%M:%S %p",
    ## 日志目录
    filename = nowPath+"/logs/transport-batch.log",
    ## 打印日志方式
    filemode = 'w'
)

@scheduler.scheduled_job("cron", day_of_week='*', hour='*', minute='*/5', second='0')
def main () :
    try:
        url2hive()
        logging.info("存储数据到hive成功")
    except:
        logging.critical("存储数据到hive失败")

if __name__ == '__main__':
    scheduler.start()
