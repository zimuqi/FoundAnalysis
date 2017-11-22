#!/usr/bin/python
# -*- coding: utf-8 -*-
import requests
import json
import re
import time
import pymysql
import demjson
from pyquery import PyQuery as pq
import traceback


def spider_rank():
    """排行榜"""
    conn = pymysql.connect(host='101.132.116.147', port=3306, user='root', passwd='liliangct', db='FoundationData',charset='utf8')
    cursor = conn.cursor()

    url = "http://fund.eastmoney.com/data/rankhandler.aspx"
    taday=time.strftime("%Y-%m-%d")
    typeArr=["hh","gp"]  # 混合型 股票型
    result = ""
    for one in typeArr:
        params = {
            "op": "ph",
            "dt": "kf",
            "ft": one,
            "rs": "",
            "gs": "0",
            "sc": "zzf",
            "st": "desc",
            "sd": taday,
            "ed": taday,
            "qdii": "",
            "tabSubtype": ", , , , ,",
            "pi": "1",
            "pn": "2000",
            "dx": "1",
            "v": "0.18434367452937206"
        }
        req=requests.get(url,params=params)
        if req.status_code==200:
            if "var" in req.content:
                data=req.content[15:][:-1].replace("%","")
                jsonData=demjson.decode(data)
                for each in jsonData["datas"]:
                    item=each.split(",")
                    print json.dumps(item,ensure_ascii=False)
                    if one=="gp":
                        foundType=0
                    if one=="hh":
                        foundType = 1
                    one=(item[0],item[1],item[3],item[4],item[5],item[6],item[7],item[8],item[9],item[10],item[11],item[12],item[13],item[14],item[18],item[22],foundType)
                    result+="('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'),"%one

    result = result[:-1]
    sql="INSERT INTO `fund_ranking`(`ades`, `jc`, `jzrq`, `dwjz`, `ljjz`, `rzdf`, `zzf`, `1yzf`, `3yzf`, `6yzf`, `1nzf`, `2nzf`, `3nzf`, `jnzf`, `lnzf`, `yh_head`,`jglx`) VALUES %s"%result
    cursor.execute(sql)
    conn.commit()

def found_detail():
    """获取基金的详细信息"""
    conn = pymysql.connect(host='101.132.116.147', port=3306, user='root', passwd='liliangct', db='FoundationData',charset='utf8')
    cursor = conn.cursor()
    cursor.execute("SELECT `ades` FROM `fund_ranking` GROUP BY `ades`")
    query=cursor.fetchall()
    for id in query:
        try:
            url="http://fund.eastmoney.com/f10/jbgk_{}.html".format(id[0])
            headers={
                "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36",
                "Referer":"http://fund.eastmoney.com/f10/ccmx_000950.html",
                "Host":"fund.eastmoney.com"
            }
            req=requests.get(url,headers=headers)
            if req.status_code==200:
                if "eastmoney" in req.text:
                    doc=pq(req.content)
                    abhtml=doc(".info.w790 tr").items()
                    info=[id[0], "","", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
                    i=1
                    for each in abhtml:
                        info[i]=each("td").eq(0).text()
                        i+=1
                        info[i] = each("td").eq(1).text()
                        i+=1
                    for each in doc(".boxitem.w790 p").items():
                        info[i] +=each.text().replace('\n','').replace('"',"'")
                        i+=1
                    info=json.dumps(info,ensure_ascii=False)[1:][:-1]
                    cursor.execute("INSERT INTO `found_introduce`(`ades`,`fn`,`jn`,`jjdm`,`jjlx`,`fxrq`,`clrq`,`zcgm`,`fegm`,`jjgl`,`jjtg`,`jjjl`,`jjfh`,`glfy`,`tgfl`,`fwf`,`rgfl`,`yjjz`,`gzbd`,`tzmb`,`tzll`,`tzfw`,`tzcl`,`fhcl`,`bjjz`,`sytz`) VALUES (%s)"%info)
                    conn.commit()
        except:
            print traceback.format_exc()
        # return

# spider_rank()
found_detail()