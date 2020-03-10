# -*- coding: utf-8 -*-
"""
该脚本应用用全局变量配置
"""
import os,sys,datetime

if __name__ == '__main__':
    print(__doc__)
    sys.exit(0)

## 全局变量
nowPath=os.path.dirname(os.path.abspath(__file__))
homePath=nowPath

appPath=os.path.join(homePath,'app')
logPath=os.path.join(homePath,'logs')
tmpPath=os.path.join(homePath,'tmp')

queryUrl='http://data.stats.gov.cn/easyquery.htm'

headersUserAgent = 'Mozilla/5.0 (Windows NT 10.0; WOW64)   \
                    AppleWebKit/537.36 (KHTML, like Gecko) \
                    Chrome/77.0.3865.120 Safari/537.36'

dfwdsValueCodeDict={
    "A0G01":"employ.json",
    "A0G02":"length.json",
    "A0G04":"traveller.json",
    "A0G05":"trantraveller.json",
    "A0G06":"freight.json",
    "A0G07":"tranfreight.json",
    "A0G08":"perdistancetraveller.json",
    "A0G09":"perdistancefreight.json",
    "A0G0F":"stationtraveller.json",
    "A0G0G":"stationfreight.json",
    "A0G0I":"civiliancar.json",
    "A0G0J":"privatecar.json",
    "A0G0K":"increaseprivatecar.json",
    "A0G0L":"highwayoperation.json",
    "A0G0O":"wharfthroughput.json",
    "A0G0P02":"wharf.json",
    "A0G0P03":"wantonwharf.json",
    "A0G0Q02":"riverwharf.json",
    "A0G0Q03":"riverwantonwharf.json",
    "A0G0T":"postal.json",
    "A0G0U":"express.json",
    "A0G0Z":"internet.json"
}
