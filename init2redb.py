# -*- coding:utf-8 _*
import sys
import numpy as np
import pandas as pd
import calendar
from datetime import datetime
from datetime import date
import time
import string
from re2sql import Mysql
import ospath
import multiprocessing
from multiprocessing import cpu_count
from sqlalchemy import create_engine

def time2hour(s,a,b):
    return round(sum((s[a] - s[b])/ np.timedelta64(1,'D')*24),2)
def indexone(s,a):
    return s.loc[s.index[0], a]

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

def worker(dbname,tablename,df,host):  
    y = Mysql(dbname = dbname,sqltype = 'postgre',host = host)
    y.insertdf(tablename,df)

class init2redb():
    ## 针对风机处理的数据库
    def __init__(self,df,turnum):
        self.df =df
        self.turnum =turnum
        
    def mod(self,s):
        y = int(s.split('-')[0])
        m = int(s.split('-')[1])
        t1 = date(y,m,21)
        if m==1:
            t0 = date(y-1,12,21)
        else:
            t0 = date(y,m-1,21)
        t = t1-t0
        return t.days*24
        
    def test(self,s):
        df1 = pd.Series()
        for i in ['cor_ora_name','tur_id','cor_province','cor_area','f_system','cor_cap_type','group_abbreviation']:
            df1[i] = indexone(s,i)
        s['f_month'] = s['f_month'].astype(str)
        t = indexone(s,'f_month')[:10]
        tmp = datetime.strptime(t,"%Y-%m-%d")
        df1['f_month'] = datetime.strftime(tmp,"%Y-%m")
        tmp = s[s['conf_debug_time']!='']
        if len(tmp)>0:
            tmp['conf_debug_time'] = tmp['conf_debug_time'].astype(str)
            t = tmp.loc[tmp.index[0],'conf_debug_time'][:10]
            tmp = datetime.strptime(t,"%Y-%m-%d")
            df1['conf_debug_time'] = datetime.strftime(tmp,"%Y-%m")
        else:
            df1['conf_debug_time'] = ''
        for i in ['f_stime','f_etime','f_repairtime']:
            s[i] = pd.to_datetime(s[i])
        df1['f_wtime'] =time2hour(s,'f_etime','f_stime')
        df1['f_wnum'] = len(s)
        s1 = s[~pd.isnull(s['f_repairtime'])]
        if len(s1) != 0:
            df1['f_rtime'] = time2hour(s1,'f_etime','f_stime')
            df1['rtime'] = time2hour(s1,'f_etime','f_repairtime')
            df1['f_rnum'] = len(s1)
            df1['f_mnum'] = len(s1['f_stime'].dt.date.drop_duplicates())
        else:
            df1['f_rtime'] = 0
            df1['rtime'] = 0
            df1['f_rnum'] = 0
            df1['f_mnum'] = 0
        datehead = 'y'+df1['f_month'].replace('-','')
        tt = self.turnum[self.turnum['cor_ora_name'] ==df1['cor_ora_name']]
        df1['month_hour'] = self.mod(df1['f_month'])
        try:
            df1['turnum'] = tt.loc[tt.index[0],datehead]
        except:
            df1['turnum'] = 0        
        return (df1)
    
    def run(self):
        result= self.df.groupby(['cor_ora_name','tur_id','f_month']).apply(self.test)
        print 'tur ansys successful'
        result = result[result['f_wtime']<720]
        return result

class init2redb1():
    ## 针对故障处理的数据库
    def __init__(self,df,turnum):
        self.df =df
        self.turnum =turnum
        
    def mody(self,s):
        y = int(s.split('-')[0])
        m = int(s.split('-')[1])
        t1 = date(y,m,21)
        if m==1:
            t0 = date(y-1,12,21)
        else:
            t0 = date(y,m-1,21)
        t = t1-t0
        return t.days
    
    def static(self,s):
        df1 = pd.Series()
        for i in ['cor_ora_name','tur_id','cor_province','cor_area','f_system','cor_cap_type','group_abbreviation','merge_f','conf_debug_time']:
            df1[i] = indexone(s,i)
        try:
            tmp = datetime.strptime(indexone(s,'f_month'),"%Y-%m-%d %H:%M:%S")
        except:
            tmp = datetime.strptime(indexone(s,'f_month'),"%Y-%m-%d")
        df1['f_month'] = datetime.strftime(tmp,"%Y-%m")
        df1['month_day'] = self.mody(df1['f_month'])
        for i in ['f_stime','f_etime']:
            s[i] = pd.to_datetime(s[i])
        df1['f_wtime'] =time2hour(s,'f_etime','f_stime')
        df1['f_wnum'] = len(s)
        datehead = 'y'+df1['f_month'].replace('-','')
        if df1["conf_debug_time"]!='':
            df1["conf_debug_time"] = pd.to_datetime(df1["conf_debug_time"]).strftime("%Y-%m")
        tt = self.turnum[self.turnum['cor_ora_name'] ==df1['cor_ora_name']]
        try:
            df1['turnum'] = tt.loc[tt.index[0],datehead]
        except:
            df1['turnum'] = 0
        return df1
        
    def run(self):
        result = self.df.groupby(['cor_ora_name','tur_id','f_month','merge_f']).apply(self.static)
        print 'fau ansys successful'
        return result


class init2redb2():
    ## 针对故障处理的数据库
    def __init__(self,df,schema):
        self.df =df
        self.plist = [0.25,0.5,1,2,4,8,24,72]
        self.pdict = {}
        for i in self.plist:
            self.pdict[i]= 0
        self.schema = schema
        try:
            self.schema.pop(self.schema.index("f_stimeh"))
        except:
            pass
        
    def mody(self,s):
        t = calendar.monthrange(int(s.split('-')[0]),int(s.split('-')[1]))[1]
        return t
    
    def static_hf(self,s):
        self.Level += 1 
        if '0' not in s:
            self.hf.extend([len(s)+2 for i in range(len(s)+2)])
            self.lf.extend([self.Level for i in range(len(s)+2)])
        elif '1' not in s:
            self.hf.extend([2,2])
            self.lf.extend([self.Level for i in range(2)])
            self.hf.extend([0 for i in range(len(s))])
            self.lf.extend([0 for i in range(len(s))])
        else:
            self.lf.extend([self.Level for i in range(len(s)+1)])
            self.lf.append(0)
            self.hf.extend([len(s)+1 for i in range(len(s)+1)])
            self.hf.append(0)
            
    def static(self,s):
        for i in ['f_stime','f_etime']:
            s[i] = pd.to_datetime(s[i])
        s['f_month'] = s['f_month'].astype(str)
        s['f_month']= s['f_month'].str.slice(0,7)     
        s['f_stimeh'] = s['f_stime'].apply(lambda x:x.hour)
        if len(s) !=1:
            # 无月份区别计算中间键标识符
            s = s.sort_values('f_stime')
            s.index = range(len(s))
            s1 = s.loc[s.index[1:],:]
            s1 = s1.append(s1.loc[s1.index[0]])
            s1.index = range(len(s1))
            s1.loc[s1.index[len(s1)-1],'f_stime'] = s1.loc[s1.index[len(s1)-1],'f_stime']+np.timedelta64(1,'Y')
            tt = s1['f_stime']-s['f_etime']
            for pl in self.plist:
                t1 = pd.Series(np.repeat(0,len(tt)))
                t1[tt<np.timedelta64(int(3600*pl),'s')] =1               
                t1[-1] = 0
                t1 = t1.sort_index()
                del (t1[len(tt)-1])
                t1.index = range(len(t1))
                t1 = t1.astype(str)
                ss = ''.join(t1.tolist())
                ss1 = ss.split('01')
                ssf = ss1.pop(0)
                self.hf = [0 for i in range(len(ssf))]
                self.lf = [0 for i in range(len(ssf))]
                t2 = pd.Series(ss1)
                self.Level = self.pdict[pl]
                t2.apply(self.static_hf)
                s['nfm'+str(pl).replace('0.','_')] = pd.Series(self.hf)
                s['nfl'+str(pl).replace('0.','_')] = pd.Series(self.lf)
            return s
        else:
            for i in self.plist:
                s['nfm'+str(i).replace('0.','_')] = 0
                s['nfl'+str(i).replace('0.','_')] = 0
            return s
        
    def run(self):
        tmp = self.df[self.schema]
        result = tmp.groupby(['cor_ora_name','tur_id','merge_f']).apply(self.static)
        print 'ref ansys successful'
        return result
    
def do_main(ops):
    multiprocessing.freeze_support()
    a = time.time()
    thr_N = cpu_count()
    x = Mysql("smart",sqltype = 'postgre',host=ops['host'])
    ## 用文件读取代替从数据库引擎中获取原始数据，可以实现增量式数据入库，各表创建标识符
    tur_bz,fau_bz,ref_bz = ops["checks"]

    ## 导入或更新机组台月数报表
    turnum = ops['redf2']
    turnum = turnum.fillna(0)
    if 'turnum' not in x.listtables():
        turnuminit = {}
        for i in turnum.columns:
            if i != 'cor_ora_name':
                turnuminit[i] = 'float'
            else:
                turnuminit[i] = 'text'
        x.creattable('turnum',turnuminit,perkey = 'cor_ora_name')
        x.insertdf('turnum',turnum)
        print u"风场台月数表导入完成"
    else:
        turnumschema = [i[2] for i in x.showschema('turnum')]
        cha = list(set(turnum.columns).difference(set(turnumschema)))
        if len(cha)!= 0:
            alterinit= {}
            for i in cha:
                alterinit[i] = 'float'
            cha.append('cor_ora_name')
            alterdf = turnum[cha]
            x.altertable('turnum',alterinit)
            for i in range(len(alterdf)):
                tmp = alterdf.loc[alterdf.index[i],:]
                condition = {'cor_ora_name':tmp['cor_ora_name']}
                data = dict(zip(tmp.index,tmp.values))
                data.pop('cor_ora_name')
                x.updatedata('turnum',data,condition)
            print u"风场台月数表更新完成"
        else:
            print u"风场台月数表未发现更新"
    
    b = time.time()
    print u"df加载完成 %0.2f" % (b-a)
    
    engine = create_engine("postgresql://root:root@%s/smart"%(args.host),encoding='utf-8')
    turnum = pd.read_sql_query('select * from init_data',con=engine)
    turnum = turnum.fillna(0)
    
    init_schema = x.showtdf('initdb_sechma')
    init_schema['chinese'] = init_schema['chinese'].map(lambda x:x.decode("utf-8"))
    init_schema = dict(zip(init_schema['chinese'],init_schema['head']))
    alldf = ops['redf1']
    alldf = alldf.fillna("")
    alldf.columns = [init_schema[i] for i in alldf.columns]
    alldf['f_month'] = alldf['f_month'].astype(str)
    
    ## 获取数据中所有年份，一年为周期的进行数据导入
    years = list(alldf['f_month'].apply(lambda x:x[:4]).drop_duplicates())
    print  "all years is :"
    print years
    
    ## 故障级数据库及重复故障数据库建立周期较长，初始化时建议分开运行
             
    for year in years:
        b = time.time()
        df = alldf[alldf['f_month'].str.find(year)!=-1]
        print u"当前处理月份为："
        print df['f_month'].drop_duplicates()
        schemainit = {'head':'varchar(200)','type':'varchar(200)','chinese':'varchar(200)'}
        ## 厂级数据库的符合主键为 风场，机组，故障月份
        if tur_bz:
            if "turschema" not in x.listtables():
                dfi = pd.read_excel("icon/turschema.xlsx")
                init = dict(zip(dfi['head'],dfi['type']))
                x.creattable('turschema',schemainit,perkey = 'head')
                x.insertdf('turschema',dfi)
                x.creattable("tur_result",init)
                print u"创建厂级故障结果库完成"
            tmp = init2redb(df,turnum)
            result = tmp.run()
            print u"%s厂级数据分析完成, 花费%0.2fs" % (year,time.time()-b)
            for i in siplitlist(result,thr_N):
                p = multiprocessing.Process(target = worker, args = ("smart","tur_result",i,ops['host'],))
                p.start()
        c = time.time()
       
        if fau_bz:
        ## 故障级数据库的符合主键为 风场，机组，故障月份,故障名称
            if "fauschema" not in x.listtables():    
                dfi = pd.read_excel("icon/fauschema.xlsx")
                init = dict(zip(dfi['head'],dfi['type']))
                x.creattable('fauschema',schemainit,perkey = 'head')
                x.insertdf('fauschema',dfi)
                x.creattable("fau_result",init)
                print u"创建故障级结果库完成"
            tmp = init2redb1(df,turnum)
            result = tmp.run()
            print u'%s故障处理完成, 花费%0.2fs'%(year,time.time()-c)
            for i in siplitlist(result,thr_N):
                p = multiprocessing.Process(target = worker, args = ("smart","fau_result",i,ops['host'],))
                p.start()

        d = time.time()
        ## 重复故障级数据库的符合主键为 风场，机组，故障月份,故障名称
        if ref_bz:
            if "refschema" not in x.listtables():
                a = time.time()
                dfi = pd.read_excel("icon/refschema.xlsx")
                init = dict(zip(dfi['head'],dfi['type']))
                x.creattable('refschema',schemainit,perkey = 'head')
                x.insertdf('refschema',dfi)
                x.creattable("ref_result" ,init)
                print u"创建重复故障数据库完成"
            schema = [i[2] for i in x.showschema("ref_result")]   
            dfi = [i for i in schema if i.find("nf")!=0]
            tmp = init2redb2(df,dfi)
            result = tmp.run()
            print u'%s重复故障处理完成，花费%0.2fs'%(year,time.time()-d)
            for i in siplitlist(result,thr_N):
                p = multiprocessing.Process(target = worker, args = ("smart","ref_result",i,ops['host'],))
                p.start()

