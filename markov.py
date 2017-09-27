# -*- coding: cp936 -*-
#-*-coding：utf-8-*-

import pandas as pd
import numpy as np
from matplotlib.font_manager import FontProperties
import random,os,time,datetime,copy
import matplotlib.pyplot as plt

def transition_matrix(df,init='s1'):
    ## 假设s1和s2,s3三种状态
    ## s1为上升，s2为下降，s3为不变(上下100)
    ## tmp为df的错位相减
    tmp = df.diff()
    tmp = tmp.fillna(0)
    tmp.index=range(len(tmp))
    tmp[tmp>100] = 's1'
    tmp[tmp<-100] = 's2'
    tmp[(tmp<100)&(tmp>-100)] = 's3'
    tmp = tmp.to_frame()
    tmp.columns = ['x']
    tmp1 = tmp.loc[1:]
    tmp1.index=range(len(tmp1))
    cs = tmp.loc[tmp.index[-1]].values[0]
    tmp.drop(tmp.index[-1],axis=0, inplace=True)
    tmp['y'] = tmp1
    df = tmp.pivot_table(index = ['x'],columns = ['y'],aggfunc = len)
    df = df.reindex(index = ['s1','s2','s3'],columns = ['s1','s2','s3'])
    df = df.fillna(0)
    df = df/(df.sum().sum())
    df = df.applymap(lambda x:round(x,3))
    return  df,cs


def Pip(tmp):
    cnt = plt.hist(tmp,bins =40)
    plt.close("all")
##    x= cnt[1][1:]
    x = cnt[1][0:-1]
    y = cnt[0]
    reck = pd.Series([(cnt[1][i]+j)/2.0 for i,j in enumerate(cnt[1][1:])])    
    z1 = np.polyfit(x, y, 7)
    p1 = np.poly1d(z1)
    yvals=p1(x)
    y_test = pd.Series(yvals)
    y_test.loc[y_test<0]=0.01
    y_test = y_test.apply(lambda x:round(x,2))
    return sum(reck*y_test/sum(y_test))
    

def distribution(df):
    tmp = df.diff()
    tmp = tmp.fillna(0)
    tmp1 = tmp[tmp>100]
    tmp2 = tmp[tmp<-100]
    tmp3 = tmp[(tmp<100)&(tmp>-100)]
    y1 = Pip(tmp1)
    y2 = Pip(tmp2)
    y3 = Pip(tmp3)
    
    ## y1和y2分别是s1和s2的概率分布函数，按照概率分布进行数据池构造进行随机数选取
    return {"s1":Pip(tmp1),"s2":Pip(tmp2),"s3":Pip(tmp3)}

def do_main(data,ps):
    font = FontProperties(fname=r"c:\windows\fonts\simsun.ttc", size=14)
    stime = time.time()
    df1 = copy.deepcopy(data)
    x1 = df1[df1.columns[0]]
    x1.index= range(len(x1))
    predict_month = ps["pdate"].strftime("%Y%m%d") ##
    df1 = df1[df1.columns[1:]]
    df1 = df1.T
    df1.index = pd.to_datetime(df1.index)
    sdate = datetime.datetime.strptime(str(ps["sdate"]),'%Y-%m-%d')
    edate = datetime.datetime.strptime(str(ps["edate"]),'%Y-%m-%d')
    df1 = df1[(df1.index<=edate)&(df1.index>=sdate)]
    df = copy.deepcopy(df1)
    df2 = pd.DataFrame(columns = df1.columns,index = pd.date_range(df1.index[-1].strftime("%Y%m%d"),predict_month,freq='M').map(lambda x:x+datetime.timedelta(days=1)))
    df1 = df1.append(df2)
    df1[~df1.isnull()] = 1
    df1 = df1.fillna(0)
    tmp = df1.diff()
    x = range(len(tmp))
    x.reverse()
    tmp.index = x
    Ns = []
    for i in tmp.columns:
        Ns.append(tmp[i][tmp[i]==-1].index[-1]+1)
    df.index =range(len(df))
    figs = []
    result = pd.DataFrame()
    if not os.path.exists("modelresult/markov"):
        os.makedirs("modelresult/markov")
    for index,col in enumerate(df.columns):
        print x1[index]
        fig = plt.figure(1)
        ax1 = plt.subplot(211)
        ax2 = plt.subplot(212)
        train = df[col].dropna()
        plt.figure(1)
        y1 = df[col].dropna().values.tolist()
        ax1.plot(y1,c='b')
        for i in range(Ns[index]):
            tranfrom,cs = transition_matrix(train)
            dis=tranfrom.loc[cs]
            ys = distribution(train)
            predict = train.iat[-1]+sum([dis[i]*ys[i] for i in dis.index])/sum(dis)
            nowcol = df1.index.strftime("%Y-%m")[len(train)]
            print "%0.2f"%predict
            train.loc[train.index[-1]+1]=round(predict,2)
        y2 = train.values.tolist()
        ax2.plot(y2,c='r')
        ax1.set_xlim(0,max([len(y1),len(y2)])+1)
        ax2.set_xlim(0,max([len(y1),len(y2)])+1)
        fig.suptitle(u'马尔科夫模型： %s '%x1[index],fontproperties=font)
        fig.savefig(u"modelresult/markov/%s.png"%(x[index]))
        figs.append(u"modelresult/markov/%s.png"%(x[index]))
        result = pd.merge(result,train.to_frame(),left_index=True,right_index=True,how="outer")
        plt.close()
    result = result.T
    result.columns = df1.index.strftime("%Y-%m")
    result.index = range(len(result.index))
    result = pd.merge(x1.to_frame(),result,left_index=True,right_index=True)
    result =  result.fillna("")
    return {"result":result,"figs":figs}
    
