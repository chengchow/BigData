# -*- coding: utf-8 -*-
"""
中国地图
"""

## 添加全局变量及函数
import os,sys
import json

## 环境变量
nowPath=os.path.dirname(os.path.abspath(__file__))
homePath=os.path.join(nowPath,'../')
sys.path.append(homePath)

## 从全局变量中引用chinamap变量
from config import chinamap,query

## 从全局函数中调用hive查询, 列表去重模块
from functions import mysql_update,load_json_file,load_yaml_file

## 获取yaml数据
yamlFile=chinamap.yamlFile
yamlDict=load_yaml_file(yamlFile)

## 获取json数据
jsonFile=query.coordQueryFile
jsonDict=load_json_file(jsonFile)

## 主程序
def main():
    ## 获取画图数据
    lineList=yamlDict.get('lineList')

    ## 索引集合(去重)
    indexSet={y.get('name') for x in lineList for y in x}
    ## 索引列表
    indexList=list(indexSet)

    ## 坐标列表
    axisList=[jsonDict.get(x) for x in indexList]

    ## 坐标输出字典
    axisDict=dict(zip(indexList,axisList))

    ## 输出字典
    outputDict={
        'axis': axisDict,
        'value': lineList
    }

    ## 转json字符串
    outputStr=json.dumps(outputDict,ensure_ascii=False)

    ## 存储结果到mysql数据库
    mysql_update(dataLabel=chinamap.label, dataValues=outputStr)

## 调试
if __name__=='__main__':
    main()
