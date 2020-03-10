# -*- coding: utf-8 -*-
"""
用于全局函数设置
"""
import os,sys
import requests
import time
import json

if __name__ == '__main__':
    print(__doc__)
    sys.exit(0)

nowPath=os.path.dirname(os.path.abspath(__file__))
homePath=os.path.join(nowPath,'../')
sys.path.append(homePath)

import config

def get_data(**kwargs):
    headers = {}
    keyValue = {}

    url=kwargs.get('Url') or config.queryUrl
    headers['User-Agent']=kwargs.get('headersUserAgent') or config.headersUserAgent
    keyValue['m']=kwargs.get('m') or 'QueryData'
    keyValue['rowcode']=kwargs.get('rowCode') or 'zb'
    keyValue['colcode']=kwargs.get('colCode') or 'sj'
    keyValue['wds']=kwargs.get('wds') or None
    keyValue['dbcode']=kwargs.get('dbCode') or 'hgnd'
    keyValue['dfwds']=kwargs.get('dfwds') or None
    keyValue['k1']=str(int(round(time.time()*1000)))

    request = requests.get(url, headers=headers, params=keyValue)

    return json.loads(request.text)
