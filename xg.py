# -*- coding: cp936 -*-
#-*-coding：utf-8-*-

import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split
from dateutil import rrule
from  datetime  import  *
from matplotlib.font_manager import FontProperties
import matplotlib.pyplot as plt
import os,copy,time
font = FontProperties(fname=r"c:\windows\fonts\simsun.ttc", size=14)
def to_time(s):
    return datetime.strptime(str(s),'%Y-%m-%d')

def do_main(data,ps):
    filldata=data.fillna(data.mean())  
    x = pd.Series(pd.to_datetime(filldata.columns.drop(filldata.columns[0])))
    x = x[(x<=to_time(ps['edate']))&(x>=to_time(ps['sdate']))]
    gm = filldata.columns[x.index+1].tolist()
    months = pd.date_range(gm[-1],ps['pdate'],freq='M').map(lambda x:x+timedelta(days=1))
    sd1=gm[0]
    sd2=gm[1]
    splot=x.index[0]+1
    eplot=x.index[-1]+2
    for i in range(len(months)):
        r=[]
        for j in xrange(len(filldata)):
            usable_columns = filldata.columns.drop(filldata.columns[0]).tolist()
            ed2=gm[-1]
            ed1=gm[-2]
            y_train = filldata.iloc[j].ix[sd2:ed2].values
            x_train = filldata.T.ix[sd1:ed1].values
            x_test = filldata.T.ix[ed2:].values
            d_train = xgb.DMatrix(x_train, label=y_train)
            d_test = xgb.DMatrix(x_test)
            params = { 
                'eta': float(ps['xg'][u'学习率'])   ,
                'max_depth': int(ps['xg'][u'最大深度']),
                'subsample': 0.9,
                'objective': 'reg:linear',
                'eval_metric': 'error',
                'silent': 0,
                'eval_metric':'logloss'
            }
            def xgb_r2_score(preds, dtrain):
                labels = dtrain.get_label()
                return 'r2', r2_score(labels, preds)
            watchlist = [(d_train, 'train')]
            clf = xgb.train(params, d_train, 1000, evals=watchlist, early_stopping_rounds=30,feval=xgb_r2_score, maximize=True, verbose_eval=10)
            p_test = clf.predict(d_test)
            r.append(round(p_test[0],2))
        currentmonth = months[i].strftime("%Y-%m")
        filldata[currentmonth]=r
        data[currentmonth]=r
        gm.append(currentmonth)
        
    path='modelresult/xg/'
    if os.path.exists(path):
        pass
    else:
        os.makedirs(path)
    p=[]
    for i in xrange(len(data)):
        fig = plt.figure(1)
        ax1 = plt.subplot(211)
        ax2 = plt.subplot(212)
        y1=data.iloc[i,splot:eplot].values.tolist()
        ax1.plot(y1,c='b')
        y2=data.loc[data.index[i],gm].values.tolist()
        ax2.plot(y2,c='r')
        ax1.set_xlim(0,max([len(y1),len(y2)])+1)
        ax2.set_xlim(0,max([len(y1),len(y2)])+1)
        fig.suptitle(u'回归预测模型： %s'%data.iat[i,0],fontproperties=font)
        fpath = u'%s/%s'%(path,i)
        plt.savefig(fpath)
        plt.close()
        p.append(fpath)
    results={'result':data,'figs':p}
    return results
