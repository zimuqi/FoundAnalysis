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
                    print(son.dumps(item,ensure_ascii=False))
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
    # query=[["502056"]]
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
                    abhtml=doc(".info.w790 td").items()
                    now=time.strftime("%Y-%m-%d %H:%M",time.localtime())
                    info=[id[0], "","", "", "", "", "", "", "", "", "", "","", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",now]
                    i=1
                    for each in abhtml:
                        if i<20:
                            if each.html():
                                if "span" in each.html():
                                    info[i]=each("span").eq(1).text()
                                elif "href" in each.html():
                                    info[i]=each("a").text()
                                else:
                                    info[i]=each.text()
                            i+=1
                        else:
                            break
                    i=21
                    for each in doc(".boxitem.w790 p").items():
                        info[i]=each.text().replace('\n','').replace('"',"'")
                        i+=1
                    info=json.dumps(info,ensure_ascii=False)[1:][:-1]

                    # print("INSERT INTO `found_introduce`(`ades`,`fn`,`jn`,`jjdm`,`jjlx`,`fxrq`,`clrq`,`zcgm`,`fegm`,`jjgl`,`jjtg`,`jjjl`,`jjfh`,`glfy`,`tgfl`,`fwf`,`rgfl`,`max_sg`,`max_sh`,`yjjz`,`gzbd`,`tzmb`,`tzll`,`tzfw`,`tzcl`,`fhcl`,`bjjz`,`sytz`,`ctime`) VALUES (%s)"%info)
                        
                    # 检查是否存在
                    cursor.execute("SELECT `id` FROM `found_introduce` WHERE `ades`={}".format(id[0]))
                    query=cursor.fetchall()
                    if query:
                        cursor.execute("REPLACE INTO `found_introduce`(`ades`,`fn`,`jn`,`jjdm`,`jjlx`,`fxrq`,`clrq`,`zcgm`,`fegm`,`jjgl`,`jjtg`,`jjjl`,`jjfh`,`glfy`,`tgfl`,`fwf`,`rgfl`,`max_sg`,`max_sh`,`yjjz`,`gzbd`,`tzmb`,`tzll`,`tzfw`,`tzcl`,`fhcl`,`bjjz`,`sytz`,`ctime`) VALUES (%s)"%info)
                        conn.commit()
                        print(u"基金id",id[0],u"更新成功")
                    else:
                        cursor.execute("INSERT INTO `found_introduce`(`ades`,`fn`,`jn`,`jjdm`,`jjlx`,`fxrq`,`clrq`,`zcgm`,`fegm`,`jjgl`,`jjtg`,`jjjl`,`jjfh`,`glfy`,`tgfl`,`fwf`,`rgfl`,`max_sg`,`max_sh`,`yjjz`,`gzbd`,`tzmb`,`tzll`,`tzfw`,`tzcl`,`fhcl`,`bjjz`,`sytz`,`ctime`) VALUES (%s)"%info)
                        conn.commit()
                        print(u"基金id",id[0],u"保存成功")
        except:
            print(u"基金id",id[0],u"更新失败")
            print(traceback.format_exc())
            # return
        # return

def found_sector_allocation():
    """基金的行业配置信息"""
    conn = pymysql.connect(host='101.132.116.147', port=3306, user='root', passwd='liliangct', db='FoundationData',charset='utf8')
    cursor = conn.cursor()
    cursor.execute("SELECT `ades` FROM `found_introduce` GROUP BY `ades`")
    query=cursor.fetchall()
    if query:
        for ades in query:
            url="http://fund.eastmoney.com/f10/F10DataApi.aspx"
            year=time.strftime("%Y",time.localtime())
            params = {
                "type": "hypz",
                "code": ades[0],
                "year": year,
                "rt":"0.3300044641223907"
            }
            headers={
                "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36",
                "Host":"fund.eastmoney.com"
            }

            req=requests.get(url,params=params,headers=headers)
            if req.status_code==200:
                if "apidata" in req.text:
                    pass




# spider_rank()
# found_detail()