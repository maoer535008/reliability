#coding : utf-8
from re2sql import Mysql
import pandas as pd
import time
import argparse
import ospath
import multiprocessing
from multiprocessing import cpu_count
import math


def siplitlist(listx,n):
    if n>len(listx):
        N = [1]*len(listx)
    else:
        a1 = len(listx)/n
        a2 = len(listx)%n
        N = [a1]*(n-a2)+[a1+1]*a2
        
    res = []
    s = 0
    for i in N:
        res.append(listx[s:s+i])
        s +=i    
    return res

def worker(dbname,q,host):
    x = Mysql(dbname = dbname,sqltype = 'postgre',host =host)
    x.exec_("SET CLIENT_ENCODING TO 'utf8'")
    N =0
    while 1:
        if not q.empty():
            N = 0
            try:
                index,df= q.get(1,5)
                x.insertdf('init_data',df)
            except Exception,e:
                print unicode(e)
            
        else:
            N+=1
            time.sleep(0.5)
        if N ==30:
            print 'thread is done'
            x.conn.close()
            break
            
def producer(q,df):
    N = int(math.floor(len(df)/10000)+1)
    for index,i in enumerate(siplitlist(df,N)):
        q.put([index,i])
    

def do_main(ops):
    multiprocessing.freeze_support()
    a = time.time()
    x = Mysql(dbname = "smart",sqltype = 'postgre',host = ops['host'])
    thr_N = cpu_count()
    if "initdb_sechma" not in x.listtables():
        schemainit = {'head':'varchar(200)','type':'varchar(200)','chinese':'varchar(200)'}
        dfi = pd.read_excel("icon/initdb_sechma.xlsx")
        init = dict(zip(dfi['head'],dfi['type']))
        x.creattable('initdb_sechma',schemainit,perkey = 'head')
        x.insertdf('initdb_sechma',dfi)
        x.creattable('init_data',init)
        print u"创建厂级故障结果库完成"        
    dfd = x.showtdf("initdb_sechma")
    dfd['chinese'] = dfd['chinese'].map(lambda x:x.decode("utf-8"))
    rhead = dict(zip(dfd['chinese'],dfd['head']))
    init = dict(zip(dfd['head'],dfd['type']))
    x.creattable('init_data',init)
    print u"数据库创建完成，正在读取数据请稍等"
    df = ops['redf1']
    df = df.fillna(value = '')
    df = df[dfd['chinese']]
    # rename 中文抬头至英文抬头对照转换
    df.columns = [rhead[i] for i in df.columns]
    b = time.time()
    print u"数据库载入成功，花费%0.2fs，正在进行数据写入，请稍等" % (b-a)
    df['f_month'] = pd.to_datetime(df['f_month'].astype(str))
    df['f_month'] = df['f_month'].dt.strftime("%Y-%m")
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target = producer, args = (q,df,))
    p.start()   
    for i in range(thr_N):
        p = multiprocessing.Process(target = worker, args = ("smart",q,ops['host'],))
        p.start()
    
