# -*- coding: utf-8 -*-
"""
交通运输、邮电通信业就业人员数
"""

## 添加全局变量及函数
import os,sys
import json

nowPath=os.path.dirname(os.path.abspath(__file__))
homePath=os.path.join(nowPath,'../')
sys.path.append(homePath)

## 从全局变量中引用employ变量
from config import employ,hive_conn

## 从全局函数中调用hive查询, 列表去重模块
from functions import spark_hive_query,list_uniq,mysql_update,load_yaml_file

## 获取yaml数据
yamlFile=employ.yamlFile
yamlDict=load_yaml_file(yamlFile)

## 获取对应dfwds.code对应的就业人数，按年份列表输出
def per_list(_year,_wds,_resultList,_div):
    _xList=[round(_x.value/_div,2) for _x in _resultList if _x.year==_year and _x.wds==_wds]
    return _xList[-1]

## 主程序
def main():
    ## 获取画图数据
    textTagList=yamlDict.get('textTagList')
    legendList=[x.get('name') for x in textTagList]
    colorList=yamlDict.get('colorList')
    titleName=yamlDict.get('titleName')
    yUnit=yamlDict.get('yUnit')
    divUnit=yamlDict.get('divUnit') or 1
    picType=yamlDict.get('picType')
    otherTagList=yamlDict.get('otherTagList')
    otherName=yamlDict.get('otherName')

    ## 定义数据库名称，表名称及查询语句
    ## 备注: 测试环境性能太差，为节省时间，这里直接查询出运输业人数总数据。
    hiveDB=employ.hiveDB or hive_conn.hiveDB
    hiveTable=employ.hiveTable or hive_conn.hiveTable

    ## 设置一个列表收集wds值
    keyList=[e.get('wds') for e in textTagList]
    ## wds集合转元组用于HQL查询
    keyTuple=tuple(keyList)

    ## HQL查询语句
    if len(keyTuple) == 1:
        HQL="SELECT wds,year,value FROM `{}`.`{}` WHERE wds='{}'".format(
             hiveDB, hiveTable,keyTuple[0])
    elif len(keyTuple) > 1:
        HQL="SELECT wds,year,value FROM `{}`.`{}` WHERE wds IN {}".format(
             hiveDB, hiveTable,keyTuple)

    ## 其他HQL查询语句
    if len(otherTagList) == 1:
        otherHQL="SELECT year,value value FROM `{}`.`{}` WHERE wds={}".format(
                  hiveDB, hiveTable,otherTagList)
    elif len(otherTagList) > 1:
        otherHQL="SELECT year,sum(value) value FROM `{}`.`{}` WHERE wds IN {} group by year".format(
                  hiveDB, hiveTable,tuple(otherTagList))

    ## 获取查询结果，输出列表，详情查看functions脚本spark_hive_query模块
    resultList=spark_hive_query(HQL=HQL)

    ## 获取other查询结果，输出列表
    otherResultList=spark_hive_query(HQL=otherHQL)

    ## 获取年份列表
    yearList=[y.year for y in resultList]

    ## 修正年份列表，添加other年份
    yearList+=[y.year for y in otherResultList]

    ## 年份列表去重
    yearList=list_uniq(yearList)

    ## 年份类别排序
    list.sort(yearList)

    ## 定义series列表
    seriesList=[]
    for x in textTagList:
        ## 获取dfwds.code的数据列表
        perList=[per_list(y,x.get('wds'),resultList,divUnit) for y in yearList]
        ## 数据汇总列表修正(汇总列表-dfwds.code列表)
        seriesList.append({
            'name': x.get('name'), 
            'type': picType, 
            'data': perList
        })

    ## 获取other列表数据
    otherPerList=[round(w.value/divUnit,2) for y in yearList for w in otherResultList if w.year==y]

    ## 数据汇总列表修正，添加other数据
    seriesList.append({
        'name': otherName,
        'type': picType,
        'data': otherPerList
    })

    ## 定义输出格式(字典)
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
    mysql_update(dataLabel=employ.label, dataValues=outputStr)

## 调试
if __name__ == '__main__':
    main()
