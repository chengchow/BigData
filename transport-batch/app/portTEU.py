# -*- coding: utf-8 -*-
"""
沿海规模以上港口(前十)分货类吞吐量
"""

## 添加全局变量及函数
import os,sys
import json

## 获取根目录位置并添加到环境变量
nowPath=os.path.dirname(os.path.abspath(__file__))
homePath=os.path.join(nowPath,'../')
sys.path.append(homePath)

## 从全局变量中引用portTEU变量
from config import portTEU,hive_conn,query

## 从全局函数中调用hive查询, 列表去重模块
from functions import spark_hive_query,mysql_update,load_yaml_file,load_json_file

## 获取yaml数据
yamlFile=portTEU.yamlFile
yamlDict=load_yaml_file(yamlFile)

## 字典列表排序取前10并追加透明部分数据
def semicircle(_list, _value):
    import copy
    ## 列表依据_value值反向排序
    _sortList=sorted(_list, key=lambda _list: _list[_value],reverse=True)
    ## 取出排序后前10数据
    _entityList=_sortList[:10]
    ## 创建透明列表
    _lucencyList=copy.deepcopy(_entityList)
    ## 透明列表修正
    [x.update(itemStyle={'color': 'transparent'},label={'show': 'false'},labelLine={'show': 'false'},name='') for x in _lucencyList]
    ## 返回前10列表和透明列表
    return _entityList+_lucencyList

def main():
    ## 获取yaml自定义画图数据
    textTagList=yamlDict.get('textTagList')
    colorList=yamlDict.get('colorList')
    yUnit=yamlDict.get('yUnit')
    divUnit=yamlDict.get('divUnit')
    picType=yamlDict.get('picType')

    ##获取hive连接信息
    hiveDB=portTEU.hiveDB or hive_conn.hiveDB
    hiveTable=portTEU.hiveTable or hive_conn.hiveTable

    ## 数据列表
    dataList=[]
    for w in textTagList:
        ## 查询条件wds
        filterKeys=w.get('code')
        ## 查询语句
        HQL="SELECT year,value FROM {db}.{tb} WHERE wds='{fk}' \
             AND year=(SELECT MAX(year) FROM {db}.{tb} WHERE wds='{fk}' AND value!=0)".format(
             db=hiveDB, tb=hiveTable, fk=filterKeys)
        ## Hive查询
        result=spark_hive_query(HQL=HQL)
        ## 查询结果换算
        totalValue=round(result[0][1]/divUnit,2)
        ## 查询结果添加到数据列表
        dataList.append({
            'name': w.get('name'),
            'value': totalValue
        })

    ## 定义排序索引的键值
    sortKey='value'
    ## 取出画图列表
    drawList=semicircle(dataList,sortKey)

    ##标签列表
    labelList=[x.get('name') for x in drawList]

    ## 生成输出字典
    outputDict={
        'color': colorList,
        'label':labelList, 
        'data': drawList
    }

    ## 转json字符串,不转码
    outputStr=json.dumps(outputDict,ensure_ascii=False)

    ## 存储结果到mysql数据库
    mysql_update(dataLabel=portTEU.label, dataValues=outputStr)

## 调试
if __name__=='__main__':
    main()
