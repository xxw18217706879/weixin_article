# -*- coding: utf-8 -*
import pymysql
import threading
import requests
import time
import random
import re
import pymongo
from config import *
from urllib.parse   import urlencode
from requests.exceptions import RequestException
from pyquery import PyQuery as pq
from bs4 import BeautifulSoup

client=pymongo.MongoClient(MONGO_URL)
db=client[MONGO_DB]
table=db[MONGO_TABLE]

headers={
    'Cookie':'sw_uuid=3028046832; sg_uuid=3060514454; dt_ssuid=9321434282; pex=C864C03270DED3DD8A06887A372DA219231FFAC25A9D64AE09E82AED12E416AC; ssuid=2958671424; SUV=00C15C23759079245A32A37FA3329226; GOTO=Af22417-3002; CXID=002D4FD97122985C7967D065D5F387A0; SUID=4C7990753320910A000000005A2EA558; ABTEST=1|1517485958|v1; IPLOC=CN3212; __guid=14337457.3688181085719619000.1517485879794.9055; SUIR=1517485964; weixinIndexVisited=1; ld=Dlllllllll2ztZbdlllllVI8xs1llllltmOnckllll9llllllylll5@@@@@@@@@@; pgv_pvi=6620488704; LSTMV=240%2C69; LCLKINT=4068; sct=4; JSESSIONID=aaayMFP1qSpkyhK7pYCew; PHPSESSID=o2i6rfvcv36mpi29ff15espid5; SNUID=79334BD3A1A4C69149D9DE58A22D632B; successCount=1|Fri, 02 Feb 2018 05:26:48 GMT; ppinf=5|1517549047|1518758647|dHJ1c3Q6MToxfGNsaWVudGlkOjQ6MjAxN3x1bmlxbmFtZTo3OkphbnVhcnl8Y3J0OjEwOjE1MTc1NDkwNDd8cmVmbmljazo3OkphbnVhcnl8dXNlcmlkOjQ0Om85dDJsdUZiUkVSSXlneVNPci1fWm0zZnVUZ1VAd2VpeGluLnNvaHUuY29tfA; pprdig=fDjQPlzz2eukMmIDH3QbblWnJTwfzNJULuqSNd3OWBCK07NTYDhY1jYMjA4z-63Eba2itRlSmVL-eEJwPvjWiNPcMhwr8eOVEaXGamVoVMNhpyYDwlsQan4fWR4BUMV0PJKrUej2ZaZW7Adv8ZzYaZC8FPSD6tuzrZmBhnWzTlw; sgid=31-33296909-AVpz9fd5nNgxRBibGlSRPaVg; ppmdig=1517549048000000bc5e8f2da410d911211c3f6ae6bce79e; monitor_count=74',
    'Host':'weixin.sogou.com',
    'Upgrade-Insecure-Requests':'1',
    'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'
}
proxy_=""
urls=[]
#请求索引页面
def get_index(url):
    global proxy_
    global urls
    try:
        if proxy_=="":#刚开始使用本地ip
            response=requests.get(url,headers=headers)
            #print(response.url)
            if response.url ==url:
                print("本地请求成功:",response.url)
                urls.append(response.url)
                return response.text
            else:
                print("反爬虫页面")
                proxy_=proxy()
                return get_index(url)
        else:
            proxies={"http://":proxy_}####使用代理
            response=requests.get(url,headers=headers,proxies=proxies)
            if response.url ==url:
                urls.append(response.url)
                print("正常请求网站:",response.url)
                print("使用代理成功:",proxy_)
                return response.text
            else:
                proxy_=proxy()
                return get_index(url)
    except ConnectionError as e:
        print(e)
        return get_index(url)

#随机取出一项代理
def proxy():
    conn = pymysql.connect(host='localhost', user='root',passwd='', db='ip',port=3306, charset='utf8')
    cursor = conn.cursor()
    sql = "SELECT * FROM available"
    cursor.execute(sql)
    res = cursor.fetchall()
    b = random.sample(res, 1)
    for a in b:
        proxy_=str((a[1]))+':'+str((a[2]))##构造代理ip
        return proxy_

#解析索引页获取url链接
def parse_index(html):
    soup=BeautifulSoup(html,'lxml')
    results=soup.findAll('h3')
    #print((results))
    for result in results:
        result=(str(result))
        pattern=re.compile('data-share="(.*?)"',re.S)#正则获取url/data-* 属性不能被获取
        hrefs=re.findall(pattern ,result)
        for href in hrefs:
            href=(href.replace("amp;",""))
            html=get_detail(href)
            results=parse_detail(html)
            for result in results:
                save_to_mongo(result)



def get_detail(href):
    try:
        response=requests.get(href)
        if response.status_code==200:
            return response.text
    except RequestException:
        print("请求详情页失败！")
        return None

#解析详情页
def parse_detail(html):
    wechat_list=[]
    soup=BeautifulSoup(html,"lxml")
    titles=soup.select("#activity-name")
    dates=soup.select("#post-date")
    nicknames=soup.select("#js_profile_qrcode > div > strong")
    texts=soup.select("#js_content")

    wechats=soup.select("#js_profile_qrcode > div > p > span")#######公众号和介绍使用列表生成
    if wechats:
        wechat_list.append(wechats[0].get_text())
        wechat_list.append(wechats[1].get_text())
    for title,date,nickname,wechat,introduction,text in zip(titles,dates,nicknames,wechat_list,wechat_list,texts):
        yield{
                'title':title.get_text().strip(),
                'date':date.get_text().strip(),
                'nickname':nickname.get_text().strip(),
                'wechat':wechat_list[0],
                'introduction':wechat_list[1].strip(),
                'text':text.get_text().replace("\xa0","").strip()
                }

#插入数据库
def save_to_mongo(result):
    if result:
        if table.insert(result):
            print("插入数据库成功！",result)
        else:
            print("插入失败！")


def main(page,keyword):
    data={
    'query':keyword,
    'type':'2',
    'page':page,
    'ie':'utf8'
}
    url="http://weixin.sogou.com/weixin?"+urlencode(data)
    #print(url)
    html=get_index(url)
    parse_index(html)


if __name__=="__main__":
    keyword="风景"
    #使用多线程
    threadpool=[]
    for i in range(0,5):
        end=20*(i+1)+1
        start=end-20
        print("线程{}开始爬取{}—{}页".format(str(i),str(start),str(end)))
        for page in range (start,end):
            th=threading.Thread(target=main,args=(page,keyword),name="线程"+str(i))
            threadpool.append(th)
            th.start()
    for th in threadpool:
        threading.Thread.join(th)
    print(urls.__len__())
    print("所有线程全部工作完成!")

