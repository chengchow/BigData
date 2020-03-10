# -*- coding: utf-8 -*-
"""
运输线路长度
"""

## 添加模块
import os,sys
import json

## 添加全局变量
nowPath=os.path.dirname(os.path.abspath(__file__))
homePath=os.path.join(nowPath,'../')
sys.path.append(homePath)

## 从全局变量中引用lengthline变量
from config import lengthline,hive_conn

## 从全局函数中调用hive查询, 列表去重模块
from functions import spark_hive_query,list_uniq,mysql_update,load_yaml_file

## 获取yaml数据
yamlFile=lengthline.yamlFile
yamlDict=load_yaml_file(yamlFile)

## 获取对应dfwds.code对应的就业人数，按年份列表输出 
def per_list(_year,_wds,_resultList,_div):
    _xList=[round(_x.value/_div,2) for _x in _resultList if _x.year==_year and _x.wds==_wds]
    return len(_xList)>=1 and _xList[-1] or None

def series_mod(**kwargs):
    ## 画图类型
    _picType=yamlDict.get('picType')
    
    ## hive查询数据
    _hiveDB=lengthline.hiveDB or hive_conn.hiveDB
    _hiveTable=lengthline.hiveTable or hive_conn.hiveTable

    ## wds数据分支名称
    _branchName=kwargs.get('branchName')

    ## 获取索引列表
    _textTagList=yamlDict.get(_branchName).get('textTagList')
    _divUnit=yamlDict.get(_branchName).get('divUnit') or 1
    _filterKeyList=[w.get('wds') for w in _textTagList]

    ## HQL查询语句
    if len(_filterKeyList) == 1:
        _HQL="SELECT wds,year,value FROM `{}`.`{}` WHERE wds='{}' ORDER BY year".format(
             _hiveDB,_hiveTable,_filterKeyList[0])
    elif len(_filterKeyList) > 1:
        _HQL="SELECT wds,year,value FROM `{}`.`{}` WHERE wds IN {} ORDER BY year".format(
             _hiveDB,_hiveTable,tuple(_filterKeyList))

    ## 数据查询
    _resultList=spark_hive_query(HQL=_HQL)

    ## 获取查询数据年份列表并去重不排序
    _yearList=[x.year for x in _resultList]
    _yearList=list_uniq(_yearList)

    ## 计算series列表
    _seriesList=[]
    for _x in _textTagList:
        _perList=[per_list(_y,_x.get('wds'),_resultList,_divUnit) for _y in _yearList]
        _seriesList.append({
           'name': _x.get('name'), 
           'type': _picType, 
           'data': _perList
        })

    ## 返回数据
    return {'year':_yearList, 'series':_seriesList}

## 主程序
def main():
    ## 获取画图数据
    colorList=yamlDict.get('colorList')
    titleName=yamlDict.get('titleName')
    yUnit=yamlDict.get('yUnit')

    ## 获取对应分支数据
    longDict=series_mod(branchName='long')
    shortDict=series_mod(branchName='short')

    ## 定义输出列表
    seriesList=longDict.get('series')+shortDict.get('series')
    yearList=list_uniq(longDict.get('year')+shortDict.get('year'))
    legendList=[x.get('name') for x in seriesList]
    outputDict={
        'title': titleName, 
        'legend': legendList, 
        'color': colorList, 
        'xlabel': yearList, 
        'yname': yUnit, 
        'series': seriesList
    }

    ## 转json字符串,不转码
    outputStr=json.dumps(outputDict,ensure_ascii=False)

    ## 存储结果到mysql数据库
    mysql_update(dataLabel=lengthline.label, dataValues=outputStr)

## 调试
if __name__ == '__main__':
    main()
