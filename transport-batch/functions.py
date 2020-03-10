# -*- coding: utf-8 -*-
"""
全局函数
"""
import os,sys
import yaml,io,json
import logging
import pymysql

from config import mysql_conn

nowPath=os.path.dirname(os.path.abspath(__file__))
homePath=os.path.join(nowPath,'../')
sys.path.append(homePath)


## yaml文件转数组
def load_yaml_file(_fileName):
    _file=open(_fileName,'r',encoding='utf-8')
    _dict=yaml.load(_file,Loader=yaml.FullLoader)
    return _dict

## json文件转数组
def load_json_file(_fileName):
    _file=io.open(_fileName,'rt',encoding='utf-8').read()
    _dict=json.loads(_file)
    return _dict

###生成唯一的UUID
#def uuid_str():
#    import uuid
#    import hashlib
#    import time
#    m=hashlib.md5()
#    m.update(bytes(str(time.time()),encoding='utf-8'))
#    return 'temp_table_'+m.hexdigest()

###hive查询
#def hive_query(**kwargs):
#    from pyhive import hive
#    from config import hive_conn
#
#    _hiveHost=kwargs.get('hiveHost') or hive_conn.hiveHost
#    _hivePort=kwargs.get('hivePort') or hive_conn.hivePort
#    _hiveUser=kwargs.get('hiveUser') or hive_conn.hiveUser
#    _hiveDB=kwargs.get('hiveDB') or hive_conn.hiveDB
#    _HQL=kwargs.get('HQL')
#
#    _conn=hive.Connection(host=_hiveHost, port=_hivePort, username=_hiveUser, database=_hiveDB)
#    _cursor=_conn.cursor()
#    _cursor.execute(_HQL)
#
#    _resultList=_cursor.fetchall()
#
#    return _resultList

def spark_hive_query(**kwargs):
    import findspark
    findspark.init()

    from pyspark.sql import SparkSession
    from config import hive_conn,spark_conn
    _sparkHost=kwargs.get('sparkHost') or spark_conn.sparkHost
    _appName=kwargs.get('appName') or spark_conn.appName
    _HQL=kwargs.get('HQL')

    _sparkSession=SparkSession.builder.master(_sparkHost).appName(_appName).enableHiveSupport().getOrCreate()

    _readDf=_sparkSession.sql(_HQL)

    _output=_readDf.collect()
    _sparkSession.stop()

    return _output

### 根据查询结果创建临时表
#def spark_tmp_table(**kwargs):
#    import findspark
#    findspark.init()
#
#    from pyspark.sql import SparkSession
#    from config import hive_conn,spark_conn
#    _sparkHost=kwargs.get('sparkHost') or spark_conn.sparkHost
#    _appName=kwargs.get('appName') or spark_conn.appName
#    _hiveTmpDB=kwargs.get('hiveTmpDB') or hive_conn.hiveTmpDB
#    _HQL=kwargs.get('HQL')
#    _hiveTmpTable=uuid_str()
#
#    _sparkSession=SparkSession.builder.master(_sparkHost).appName(_appName).enableHiveSupport().getOrCreate()
#
#    _sparkSession.sql("create database if not exists "+_hiveTmpDB)
#    _sparkSession.sql("drop table if exists "+_hiveTmpDB+"."+_hiveTmpTable)
#    _sparkSession.sql("create table "+_hiveTmpDB+"."+_hiveTmpTable+" "+_HQL)
#
#    return _hiveTmpTable

##    _readDf=_sparkSession.sql(_HQL)
##    return _readDf.collect()

### 删除临时表
#def spark_stop_table(**kwargs):
#    import findspark
#    findspark.init()
#
#    from pyspark.sql import SparkSession
#    from config import hive_conn,spark_conn
#    _sparkHost=kwargs.get('sparkHost') or spark_conn.sparkHost
#    _appName=kwargs.get('appName') or spark_conn.appName
#    _hiveTmpDB=kwargs.get('hiveTmpDB') or hive_conn.hiveTmpDB
#    _hiveTmpTable=kwargs.get('hiveTmpTable')
#
#    _sparkSession=SparkSession.builder.master(_sparkHost).appName(_appName).enableHiveSupport().getOrCreate()
#
#    _sparkSession.sql("drop table if exists "+_hiveTmpDB+"."+_hiveTmpTable)


##列表去重不排序
def list_uniq (_list):
    from functools import reduce
    func=lambda x,y:x if y in x else x + [y]
    return reduce(func, [[], ] + _list)

##列表去重排序
def list_sort(_list):
    return list(set(_list))

# mysql查询
def mysql_query(**kwargs):
    _mysqlCmd=kwargs.get('mysqlCmd') or mysql_conn.mysqlCmd
    _mysqlHost=kwargs.get('mysqlHost') or mysql_conn.mysqlHost
    _mysqlUser=kwargs.get('mysqlUser') or mysql_conn.mysqlUser
    _mysqlPass=kwargs.get('mysqlPass') or mysql_conn.mysqlPass
    _mysqlPort=kwargs.get('mysqlPort') or mysql_conn.mysqlPort
    _mysqlDB=kwargs.get('mysqlDB') or mysql_conn.mysqlDB
    _mysqlCharset='utf8'
    try:
        _db=pymysql.connect(host=_mysqlHost,user=_mysqlUser,passwd=_mysqlPass,port=_mysqlPort,db=_mysqlDB,charset=_mysqlCharset)
        _cursor=_db.cursor()
        _sql=_mysqlCmd
        try:
            _cursor.execute(_sql)
            _result=_cursor.fetchall()
            _outputValue=_result
            _cursor.close()
        except Exception as e:
            logging.error("执行命令 "+_mysqlCmd+" 失败. "+e)
        _db.close()
        return _outputValue
    except Exception as e:
        logging.error("数据库连接失败. "+e)

## mysql修改
def mysql_modify(**kwargs):
    _mysqlCmd=kwargs.get('mysqlCmd') or mysql_conn.mysqlCmd
    _mysqlHost=kwargs.get('mysqlHost') or mysql_conn.mysqlHost
    _mysqlUser=kwargs.get('mysqlUser') or mysql_conn.mysqlUser
    _mysqlPass=kwargs.get('mysqlPass') or mysql_conn.mysqlPass
    _mysqlPort=kwargs.get('mysqlPort') or mysql_conn.mysqlPort
    _mysqlDB=kwargs.get('mysqlDB') or mysql_conn.mysqlDB
    _mysqlCharset='utf8'
    try:
        _db=pymysql.connect(host=_mysqlHost,user=_mysqlUser,passwd=_mysqlPass,port=_mysqlPort,db=_mysqlDB,charset=_mysqlCharset)
        _cursor=_db.cursor()
        _sql=_mysqlCmd
        try:
            _cursor.execute(_sql)
            _db.commit()
            _cursor.close()
        except Exception as e:
            logging.error("执行命令 "+_mysqlCmd+" 失败. "+e)
            _db.rollback()
        _db.close()
    except Exception as e:
        logging.error("数据库连接失败. "+e)

def mysql_update(**kwargs):
    _mysqlHost=kwargs.get('mysqlHost') or mysql_conn.mysqlHost
    _mysqlUser=kwargs.get('mysqlUser') or mysql_conn.mysqlUser
    _mysqlPass=kwargs.get('mysqlPass') or mysql_conn.mysqlPass
    _mysqlPort=kwargs.get('mysqlPort') or mysql_conn.mysqlPort
    _mysqlDB=kwargs.get('mysqlDB') or mysql_conn.mysqlDB
    _mysqlTable=kwargs.get('mysqlTable') or mysql_conn.mysqlTable
    _dataLabel=kwargs.get('dataLabel')
    _dataValues=kwargs.get('dataValues')

    _selectCmd="SELECT name FROM `{table}` WHERE name='{name}'".format(
              table=_mysqlTable, name=_dataLabel)
    _insertCmd="INSERT INTO `{table}` ({col1},{col2}) VALUES ('{name}','{data}')".format(
              table=_mysqlTable, col1='name', col2='data', name=_dataLabel, data=_dataValues)
    _updateCmd="UPDATE `{table}` SET data='{data}' WHERE name='{name}'".format(
              table=_mysqlTable, data=_dataValues, name=_dataLabel)

    ## 查询结果是否存在
    selectResult=mysql_query(
        mysqlCmd=_selectCmd,
        mysqlHost=_mysqlHost, 
        mysqlUser=_mysqlUser, 
        mysqlPass=_mysqlPass, 
        mysqlPort=_mysqlPort, 
        mysqlDB=_mysqlDB
    )

    ## 修改结果
    if selectResult:
        ## 修改数据命令
        _cmd=_updateCmd
    else:
        ## 插入数据命令
        _cmd=_insertCmd

    ## 更新数据
    mysql_modify(
        mysqlCmd=_cmd,
        mysqlHost=_mysqlHost, 
        mysqlUser=_mysqlUser, 
        mysqlPass=_mysqlPass, 
        mysqlPort=_mysqlPort, 
        mysqlDB=_mysqlDB
    )

if __name__ == '__main__':
    print(__doc__)
#    print(hive_query(HQL='select wds,value,year from transport limit 10'))
#    print(spark_hive_query(HQL="select wds,year,value from bigdata.transport where wds like 'A0G01%' order by year"))
