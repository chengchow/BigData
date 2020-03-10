# -*- coding: utf-8 -*-
"""
定时处理hive数据，结果输出到mysql
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

## 堵塞方式运行计划任务
scheduler = BlockingScheduler()

## 调用日志格式
config.log

@scheduler.scheduled_job("cron", day_of_week='*', hour='0', minute='0', second='0')
def main () :
    fileList=os.listdir(appPath)
    moduleList=[x.replace('.py','') for x in fileList]

    for m in moduleList:
        importCmd='from {0} import {1} as {0}'.format(m,'main')
        exceptMsg='{}不存在'.format(m)
        execCmd='{}()'.format(m)
        errorLog='{}数据获取失败'.format(m)
        infoLog='{}数据获取成功'.format(m)

        try:
            exec(importCmd)
        except Exception as e:
            logging.warnning(exceptMsg,e)
        else:
            try:
                exec(execCmd)
            except Execption as e:
                logging.error(errorLog)
            else:
                logging.info(infoLog)

if __name__ == '__main__':
    scheduler.start()
