#coding=utf-8
import pandas as pd
import numpy as np
import calendar
from re2sql import Mysql
import time
import numpy as np
import ospath

class mdata_ansys():
    def __init__(self,df):
        self.df = df      
                          
    def indexone(self,s,a):
        return s.loc[s.index[0], a]

    def judgeagg(self,df,agg):
        if agg!= '':
            return list(df[agg].drop_duplicates())
        else:
            return['']             

    def judgelist(self,df,agg,ax):
        # df 针对单元素或数组的筛选
        if not isinstance(ax,list):
            return df[df[agg] == ax]
        else:
            return df[df[agg].isin(ax)]

    def judgeaxis(self,axis):
        # 判断双轴图还是单轴图
        if not isinstance(axis,list) or (isinstance(axis,list) and len(axis) == 1):
            if isinstance(axis,list):
                tmp = axis
            else:
                tmp = [axis]
            return (tmp,1)
        elif len(axis) == 2:
            return (axis,2)
        else:
            return ([],-1)  

    def dfturnum(self,df):
        ## 台月数计算方法
        df1 = df.drop_duplicates('f_month')
        tmpdf = self.turnum[self.turnum['cor_ora_name'].isin(df['cor_ora_name'].drop_duplicates())]
        ydf = pd.DataFrame(np.repeat('y',len(df1)),columns = ['y'])
        f_month = ydf['y'].str.cat(df1['f_month'].str.replace('-',''))
        tmpdf = tmpdf[f_month]
        return [tmpdf.sum().sum(),len(df)]
  
    def groupstatic(self, xagg ,yagg,converge,lines,repeattime):
        ## 决定了df的分组输出情况,数据组切片
        col = [xagg ,yagg]
        if (u'重复故障时间段' in col) and (u'重复故障次数' in col):
            if repeattime ==u'':
                result = []
                turnum = []
                legends = []
            else:
                tt = (xagg if xagg == u'重复故障次数' else yagg)
                if xagg == u'重复故障次数':
                    self.xagg =u'重复故障时间段'
                    self.yagg =u'重复故障次数'
                else:
                    self.xagg =u'重复故障次数'
                    self.yagg =u'重复故障时间段'
                tt1 = (converge if xagg == u'重复故障次数' else lines)
                xlegend = (self.xlegend if xagg == u'重复故障次数' else self.legend)
                if tt1 == [[]]:
                     tt1 = self.judgeagg(self.df,repeattime)
                     xlegend = tt1
                result = [[self.judgelist(self.df,repeattime,i)[['cor_ora_name','f_month',repeattime]]] for i in tt1]
                turnum = [self.dfturnum(self.df) for i  in range(len(result))]
                legends = [[i,[]] for i in xlegend]

                
        elif (u'重复故障时间段' in col) and (u'重复故障次数' not in col):
            tt = (yagg if xagg == u'重复故障时间段' else xagg)
            tt1 = (converge if xagg == u'重复故障时间段' else lines)
            tt2 = (lines if xagg == u'重复故障时间段' else converge)
            xlegend = (self.legend if xagg == u'重复故障时间段' else self.xlegend)
            ## tt1 是要筛选的时间段的字段表
            if tt1 == [[]]:
                if repeattime ==u'':
                    tt1 = [i for i in self.df.columns if i.find("nfm")==0 ]
                else:
                    tt1 = [repeattime]    
            else:
                tmp = []
                for i in tt1:
                    if isinstance(i,list):
                        tmp.append(i[0])
                    elif isinstance(i,unicode):
                        tmp.append(i)
                    else:
                        pass
                tt1 = tmp
            if xagg == u'重复故障时间段':
                self.yagg =u'重复故障时间段'
                self.xagg =tt
            else:
                self.yagg =tt
                self.xagg =u'重复故障时间段'
            if xlegend ==[]:
                xlegend = self.judgeagg(self.df,tt)
            if tt ==u'':
                result = [[self.df[['cor_ora_name','f_month',i]]] for i in tt1]
                turnum = [self.dfturnum(self.df) for i  in range(len(result))]
                legends = [[i,xlegend] for i in tt1]
            else:
                result = []
                turnum = []
                legends= []
                if tt2 == [[]]:
                    tt2 = self.judgeagg(self.df,tt)
                for i in tt1:
                    tmpdf = self.df[list(set(['cor_ora_name','f_month',tt,i]))]
                    result.append([self.judgelist(tmpdf,tt,i) for i in tt2])
                    turnum.append(self.dfturnum(tmpdf))
                legends = [[i,xlegend] for i in tt1]
                    
        elif (u'重复故障时间段' not in col) and (u'重复故障次数' in col):
            if repeattime ==u'':
                result = []
                turnum = []
                legends = []
            else:
                tt = (yagg if xagg == u'重复故障次数' else xagg)
                if xagg == u'重复故障次数':
                    self.xagg =u'重复故障次数'
                    self.yagg =tt
                else:
                    self.xagg =tt
                    self.yagg =u'重复故障次数'                   
                tt1 = (converge if xagg == u'重复故障次数' else lines)
                xlegend = (self.xlegend if xagg == u'重复故障次数' else self.legend)
                legend = (self.legend if xagg == u'重复故障次数' else self.xlegend)
                tt2 = (lines if xagg == u'重复故障次数' else converge)
                if tt ==u'':
                    if tt1 == [[]]:
                        tt1 = self.judgeagg(self.df,repeattime)
                        xlegend = tt1
                    result = [[self.judgelist(self.df,repeattime,i)[[repeattime]]] for i in tt1]
                    turnum = [self.dfturnum(self.df) for i  in range(len(result))]
                    legends = [[i,[]] for i in xlegend]

                else:
                    result = []
                    turnum = []
                    legends = []
                    bz = (tt2 == [[]])
                    bz1 = (tt1 == [[]])
                    tmpdf = self.df[list(set(['cor_ora_name','f_month',tt,repeattime]))]
                    if xagg == u'重复故障次数':
                        if bz1:
                            tt1 = self.judgeagg(tmpdf,repeattime)
                            xlegend = tt1
                        for index,i in enumerate(tt1):
                            ttmpdf = self.judgelist(tmpdf,repeattime,i)
                            if bz:
                                tt2 = self.judgeagg(ttmpdf,tt)
                                legend = tt2
                            result.append([self.judgelist(ttmpdf,tt,j) for j in tt2])
                            turnum.append(self.dfturnum(ttmpdf))
                            legends.append([xlegend[index],legend])
                    else:
                        if bz:
                            tt2 = self.judgeagg(tmpdf,tt)
                            legend = tt2
                        for index,i in enumerate(tt2):
                            ttmpdf = self.judgelist(tmpdf,tt,i)
                            if bz1:
                                tt1 = self.judgeagg(ttmpdf,repeattime)
                                xlegend = tt1
                            result.append([self.judgelist(ttmpdf,repeattime,j) for j in tt1])
                            turnum.append(self.dfturnum(ttmpdf))
                            legends.append([legend[index],xlegend])
                            
        else:
            self.xagg = xagg
            self.yagg = yagg
            if repeattime ==u'':
                result = []
                turnum = []
                legends = []
            else:
                if col == ['','']:
                    result = [[self.df[['cor_ora_name','f_month',repeattime]]]]
                    turnum = [self.dfturnum(self.df)]
                    legends = [['total',[]]]
                    self.xagg =u'total'
                    self.yagg =u'total'
                    
                elif u'' in col:
                    tt = (yagg if xagg == u'' else xagg)
                    tt1 = (lines if xagg == u'' else converge)
                    xlegend = (self.legend if xagg == u'' else self.xlegend)
                    tmpdf = self.df[list(set(['cor_ora_name','f_month',tt,repeattime]))]
                    if tt1 == [[]]:
                        tt1 = self.judgeagg(tmpdf,tt)
                        xlegend = tt1
                    result = [[self.judgelist(tmpdf,tt,i)] for i in tt1]
                    turnum = [self.dfturnum(tmpdf) for i in range(len(result))]
                    legends = [[i,[]] for i in xlegend]
                else:
                    result = []
                    turnum = []
                    legends = []
                    bz = (lines == [[]])
                    tmpdf = self.df[list(set(['cor_ora_name','f_month',xagg,yagg,repeattime]))]
                    if converge == [[]]:
                        converge = self.judgeagg(tmpdf,xagg)
                        self.xlegend =  converge
                    for index,i in enumerate(converge):
                        ttmpdf = self.judgelist(tmpdf,xagg,i)
                        if bz:
                            lines = self.judgeagg(ttmpdf,yagg)
                            self.legend = lines
                        result.append([self.judgelist(ttmpdf,yagg,j) for j in lines])
                        turnum.append(self.dfturnum(ttmpdf))
                        legends.append([self.xlegend[index],self.legend])                    
        return result,turnum,legends


    def mindf(self,df,turnum,demoncheck):
        ## 计算最小处理单元的指标计算 ,demoncheck为False采用算法1,即turnum和数组长度是根据最小处理单元的台月数和长度得出，若为True则根据给定turnum
        if len(df) != 0 :
            if demoncheck == False:
                turnum = self.dfturnum(df)
            recol = [i for i in df.columns if i.find("nf") ==0][0]
            Cft = float(len(df[~(df[recol]==0)]))  ## 重复故障频次分子即 重复标识非零行数
            Cfe_member = float(len(df))  ## 重复故障程度分子即 重复标识次数统计值
            Cff = Cft/turnum[0]
            Cfp = (Cft/turnum[1])*100
            Cfef = Cfe_member/turnum[0]
            Cfep = (Cfe_member/turnum[1])*100
            re = pd.DataFrame([[Cff,Cfp,Cfef,Cfep,Cft]],columns = ['Cff','Cfp','Cfef','Cfep','Cft'])
        else:
            re = pd.DataFrame([[0,0,0,0,0]],columns = ['Cff','Cfp','Cfef','Cfep','Cft'])
        return re

    def res(self,dfs,turnum,legends,demoncheck):
        result = pd.DataFrame()
        result.index.name = 'id'
        for index in range(len(dfs)):
            legend = legends[index]
            res = pd.DataFrame()
            for df in dfs[index]:
                ## dfs[index]为 Y轴切片的df组
                res = res.append(self.mindf(df,turnum[index],demoncheck))
            if legend[1]!=[]:
                res.index = legend[1]
            else:
                res.index = range(len(res))
            res.index.name = self.yagg
            res[self.xagg] =  legend[0]
            result = result.append(res.set_index([res[self.xagg], res.index],  drop=False))
        del result[self.xagg]
        return result

    def cftrans(self,s):
        s = (s/float(s.sum()))*100
        return s
              
    def ansys_plot(self,axis,turnum = [] , repeattime = '' ,demoncheck = False, ylabel = [],yagg = '',lines = [[]],figuretype = 'line',legend = [],converge = [[]],xlegend = [],xagg = '',target = [],xlabel = u'月份',title = '',figname = 'test.png'):
        ax,axN = self.judgeaxis(axis)
        if axN == -1:
            print u'指标过多，无法计算'
        else:    
            self.legend = legend
            self.xlegend = xlegend
            self.turnum = turnum
            dfs,turnum,legends = self.groupstatic(xagg,yagg,converge,lines,repeattime)
            '''
            dfs 为 groupstatic 根据xagg，yagg和retime的取值组合抽取合并数据 将所选retime进行抽取分组，通过converge,lines对df进行切片，输出为[[]]
            res 是对结果dfs的处理，每个最小单元送至mindf处理，mindf 为最小数据处理单元，计算5个指标，指标计算中涉及到算法1和算法2，其台月数的获取是参数输入
            Cft= 重复故障时钟分布：某一时刻的重复故障时间下的重复标识非零行数 时钟分布的占比值（%）
            Cff=重复故障频次 :某一时刻的重复故障时间下的重复标识非零行数/总台月数
            Cfp=重复故障占比 :某一时刻的重复故障时间下的重复标识非零行数/总行数的占比值（%）
            Cfef=重复程度频次 :某一时刻的重复故障时间下的重复标识次数统计值/总台月数
            Cfep=重复程度占比 :某一时刻的重复故障时间下的重复标识次数统计值/总行数的占比值（%)
            axis [] 决定了Y轴数量，主要针对指标最多2个 ([计算指标名称]) ********
            yagg  决定曲线绘制标识 单值 ********
            lines [[]] 决定了单个指标绘制的线数，主要针对yagg标识聚合
            legend []  决定了单个指标绘制的图例，数量与lines保存一致
            converge [[]] X轴的聚合，决定了X方向的点数，主要针对xagg标识聚合

            '''
            if len(dfs)!= 0:
                print u'开启计算'
                result = self.res(dfs,turnum,legends,demoncheck)
                print result
                redf1 = result[ax[0]].unstack()
                print redf1
                if ax[0] == u'Cft' and  (u'f_stimeh' in [xagg,yagg]):
                    N = (0 if xagg == u'f_stimeh' else 1)
                    redf1 = redf1.apply(self.cftrans,axis = N)
                    redf1 = (redf1 if xagg == u'f_stimeh' else redf1.T)
                print redf1
                if u'重复故障时间段' in [xagg,yagg]:
                    redf1 = redf1.T
                redf2 = pd.DataFrame()
                return {'redf':result,'redf1':redf1,'redf2':redf2}
                ## res中处理得到的df即为中间键 可以输出保存
            else:
                print u'参数设定有误，无法启动计算'

          
        
