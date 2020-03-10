# -*- coding: utf-8 -*-
"""
全局变量设置
"""
import os
import logging

## 全局变量
nowPath=os.path.dirname(os.path.abspath(__file__))
homePath=nowPath

appPath=os.path.join(homePath,'app')
logPath=os.path.join(homePath,'logs')
yamlPath=os.path.join(homePath, 'yaml')
queryPath=os.path.join(homePath,'query')

class query():
    coordQueryFile=os.path.join(queryPath,'coordinates.json')
    colorQueryFile=os.path.join(queryPath,'color.json')

## 初始化日志
"""
format: 指定输出的格式和内容，format可以输出很多有用信息，如上例所示:
    %(levelno)s: 打印日志级别的数值
    %(levelname)s: 打印日志级别名称
    %(pathname)s: 打印当前执行程序的路径，其实就是sys.argv[0]
    %(filename)s: 打印当前执行程序名
    %(funcName)s: 打印日志的当前函数
    %(lineno)d: 打印日志的当前行号
    %(asctime)s: 打印日志的时间
    %(thread)d: 打印线程ID
    %(threadName)s: 打印线程名称
    %(process)d: 打印进程ID
    %(message)s: 打印日志信息
"""
log=logging.basicConfig(
    ## 日志级别: DEBUG, INFO, WARNNING, ERROR, CRITICAL, 默认是WARNNING
    level = logging.INFO,
    ## 日志格式: 时间, 代码所在文件名, 代码行号, 日志级别名字, 日志信息
    format = "%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s",
    ## 日志打印时间
    datefmt = "%Y/%m/%d %H:%M:%S %p",
    ## 日志目录
    filename = nowPath+"/logs/transport-batch.log",
    ## 打印日志方式 w 或 a
    filemode = 'a'
)

## hive连接
class hive_conn():
    hiveHost='bigdata121'
    hivePort=10000
    hiveUser='net100'
    hiveDB='bigdata'
    hiveTable='transport'
    hiveTmpDB='hive_tmp'

## mysql连接
class mysql_conn():
    mysqlHost='localhost'
    mysqlUser='root'
    mysqlPass='R00t@M2r12db'
    mysqlPort=3306
    mysqlDB='bigdata'
    mysqlTable='transport'

## spark连接
class spark_conn():
    sparkHost="spark://bigdata121:7077"
    appName="zzu"

## 就业人员信息 
class employ():
    label='employ'
    yamlFile=os.path.join(yamlPath,'employ.yaml')
    hiveDB=None
    hiveTable=None
    mysqlDB=None
    mysqlTable=None

## 运输线长度
class lengthline():
    label='lengthline'
    yamlFile=os.path.join(yamlPath,'lengthline.yaml')
    hiveDB=None
    hiveTable=None
    mysqlDB=None
    mysqlTable=None

## 货物运输平均长度
class lengthfreight():
    label='lengthfreight'
    yamlFile=os.path.join(yamlPath,'lengthfreight.yaml')
    hiveDB=None
    hiveTable=None
    mysqlDB=None
    mysqlTable=None

## 旅客运输平均长度
class lengthtraveller():
    label='lengthtraveller'
    yamlFile=os.path.join(yamlPath,'lengthtraveller.yaml')
    hiveDB=None
    hiveTable=None
    mysqlDB=None
    mysqlTable=None

## 汽车拥有量
class motor():
    label='motor'
    yamlFile=os.path.join(yamlPath,'motor.yaml')
    hiveDB=None
    hiveTable=None
    mysqlDB=None
    mysqlTable=None

## 快递业务量
class express():
    label='express'
    yamlFile=os.path.join(yamlPath,'express.yaml')
    hiveDB=None
    hiveTable=None
    mysqlDB=None
    mysqlTable=None

class internet():
    peopleLabel='netpeople'
    portLabel='netport'
    familyLabel='netfamily'
    peopleYamlFile=os.path.join(yamlPath,'netpeople.yaml')
    portYamlFile=os.path.join(yamlPath,'netport.yaml')
    familyYamlFile=os.path.join(yamlPath,'netfamily.yaml')
    hiveDB=None
    hiveTable=None
    mysqlDB=None
    mysqlTable=None

class postal():
    label='postal'
    yamlFile=os.path.join(yamlPath,'postal.yaml')
    hiveDB=None
    hiveTable=None
    mysqlDB=None
    mysqlTable=None

class portberth():
    label='portberth'
    yamlFile=os.path.join(yamlPath,'portberth.yaml')
    hiveDB=None
    hiveTable=None
    mysqlDB=None
    mysqlTable=None

class portTEU():
    label='portTEU'
    yamlFile=os.path.join(yamlPath,'portTEU.yaml')
    hiveDB=None
    hiveTable=None
    mysqlDB=None
    mysqlTable=None

class portWDB():
    label='portWDB'
    yamlFile=os.path.join(yamlPath,'portWDB.yaml')
    hiveDB=None
    hiveTable=None
    mysqlDB=None
    mysqlTable=None

class railwayfreight():
    label='railwayfreight'
    yamlFile=os.path.join(yamlPath,'railwayfreight.yaml')
    hiveDB=None
    hiveTable=None
    mysqlDB=None
    mysqlTable=None

class railwaytraveller():
    label='railwaytraveller'
    yamlFile=os.path.join(yamlPath,'railwaytraveller.yaml')
    hiveDB=None
    hiveTable=None
    mysqlDB=None
    mysqlTable=None

class traffic():
    label='traffic'
    yamlFile=os.path.join(yamlPath,'traffic.yaml')
    hiveDB=None
    hiveTable=None
    mysqlDB=None
    mysqlTable=None

class chinamap():
    label='chinamap'
    yamlFile=os.path.join(yamlPath,'chinamap.yaml')
    hiveDB=None
    hiveTable=None
    mysqlDB=None
    mysqlTable=None

class chinarailway():
    label='chinarailway'
    yamlFile=os.path.join(yamlPath,'chinarailway.yaml')
    hiveDb=None
    hiveTable=None
    mysqlDb=None
    mysqlTable=None

if __name__ == '__main__':
    print(__doc__)
