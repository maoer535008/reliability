#coding=utf-8
from re2sql import Mysql
from sqlalchemy import create_engine
import pandas as pd

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

x =Mysql(host = "10.76.13.97",sqltype= "postgre")
y =Mysql(host = "10.2.12.251",sqltype= "postgre")

for i in x.listtables():
    print "current table is :",i
    df = x.showtdf(i)
    if ("sechma" in i) or ("schema" in i):
        init = {"head":"text","type":"text","chinese":"text"}
        y.creattable(i,init,perkey ="head")
        y.insertdf(i,df)
        
    else:
        init1 = zip(*x.showschema(i))
        init1 = dict(zip(init1[2],init1[1]))
        y.creattable(i,init1)
        for j in siplitlist(df,20):
            y.insertdf(i,j)
