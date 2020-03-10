# -*- coding: utf-8 -*-
"""
1. 从url爬取数据处理后写入local
2. local数据上传到hdfs
3. hdfs数据处理后写入hive
"""

import os,sys,json,time
import logging
import pandas as pd
from hdfs.client import Client
from pyhive import hive

nowPath=os.path.dirname(os.path.abspath(__file__))
homePath=os.path.join(nowPath,'../')
sys.path.append(homePath)

import config
import functions

localTmpPath=config.tmpPath
localTmpFile=os.path.join(localTmpPath,'transport.csv')
hdfsTmpFile='/transport/tmp/transportition.csv'

client=Client("http://bigdata121:9870")
#client.makedirs(localTmpPath)

## 程序部分
def db_query(**kwargs):
    _host=kwargs.get('host') or 'localhost'
    _port=kwargs.get('port') or 10000
    _userName=kwargs.get('userName') or 'net100'
    _dataBase=kwargs.get('dataBase') or 'default'
    _cmd=kwargs.get('cmd')

    try:
        _conn=hive.Connection(host=_host, port=_port, username=_userName, database=_dataBase)
        logging.info("数据库连接成功")
        _cursor=_conn.cursor()
    except Exception as e:
        logging.error("数据库连接失败")

    try:
        _cursor.execute(_cmd)
        logging.info('数据库数据插入成功')
    except Exception as e:
        logging.warning('数据库数据插入失败，开始重置数据库')
        dropTable="drop table transport"
        createTable="create table if not exists transport ( \
                     code string, \
                     value float, \
                     wds string, \
                     year int, \
                     unixtime int ) \
                     row format delimited fields terminated by ',' \
                     tblproperties('skip.header.line.count'='1') \
                    "
        try:
            _cursor.execute(dropTable)
            logging.warning('删除表成功')
        except:
            logging.warning('删除表失败')
        _cursor.execute(createTable)
        logging.info('重置数据库成功')
        _cursor.execute(_cmd)
        logging.info('数据库数据插入成功')

#    _outputList=_cursor.fetchall()
#    _conn.close()
#    return _outputList

def url_query(**kwargs):
    _unixTime=int(time.time())
    _url=config.queryUrl
    _headersUserAgent=config.headersUserAgent
    _m=kwargs.get('m') or 'QueryData'
    _rowCode=kwargs.get('rowCode') or 'zb'
    _colCode=kwargs.get('colCode') or 'sj'
    _dbCode=kwargs.get('dbCode') or 'hgnd'
    _wds=kwargs.get('wds') or '[]'
    _dfwds=kwargs.get('dfwds')

    _data=functions.get_data(url=_url, headersUserAgent=_headersUserAgent, m=_m, rowCode=_rowCode,
                           colCode=_colCode, dbCode=_dbCode, wds=_wds, dfwds=_dfwds)
    logging.info('爬取数据'+_dfwds+'成功')

    _list=[]
    if int(_data.get('returncode')) < 400 :
        for d in _data.get('returndata').get('datanodes'):
            _code=d.get('code')
            _data=float(d.get('data').get('data'))
            _wds=d.get('wds')[0].get('valuecode')
            _year=int(d.get('wds')[1].get('valuecode'))
            _tuple=(_code, _data, _wds, _year, _unixTime)
            _list.append(_tuple)
    return _list

def url2hadoop():
    m='QueryData'
    rowCode='zb'
    colCode='sj'
    dbCode='hgnd'
    wds='[]'
    dfwdsValueCodeDict=config.dfwdsValueCodeDict

    totalList=[]
    for i in dfwdsValueCodeDict:
        dfwds='[{"wdcode":"zb","valuecode":"' + i + '"}]'
        dataList=url_query(m=m, rowCode=rowCode, colCode=colCode, dbCode=dbCode, wds=wds, dfwds=dfwds)
        totalList+=dataList

    name=['code','value', 'wds', 'year', 'unixtime']
    PD=pd.DataFrame(columns=name, data=totalList)
    PD.to_csv(localTmpFile, index=False)
    logging.info('数据写入本地成功')

    client.upload(hdfsTmpFile, localTmpFile,cleanup=True) 
    logging.info('数据上传hadoop成功')

def hadoop2hive():
    host='bigdata121'
    port='10000'
    userName='net100'
    dataBase='bigdata'
    cmd='LOAD DATA INPATH \"'+hdfsTmpFile+'\" OVERWRITE INTO TABLE transport'

    db_query(host=host, port=port, userName=userName, dataBase=dataBase, cmd=cmd)

def main ():
    url2hadoop()
    hadoop2hive()

if __name__ == '__main__':
    main()
