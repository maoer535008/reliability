# -*- coding: utf-8 -*-
import pandas as pd
import pylab as pl
import numpy as np
from pylab import mpl
import calendar
from re2sql import Mysql
import time
import numpy as np
import ospath

class mdata_ansys():
    def __init__(self,df):
        self.df = df
        ########## 解决pandas中文标签显示#########
        mpl.rcParams['font.sans-serif'] = ['SimHei']
        mpl.rcParams['axes.unicode_minus'] = False
        ########## 解决pandas中文标签显示#########        
                          
    def indexone(self,s,a):
        return s.loc[s.index[0], a]

    def PSIL(self,g,PSIL_value):
        if PSIL_value == None:
            begine = self.indexone(g,'FF')
        else:
            begine = PSIL_value
        g['PSIL'] = (begine - g['FF'])/begine*100
        return g
        
    def groupstatic(self,df =None,agg = ''):
        if not df:
            df = self.df
        if not isinstance(agg,list):
            agg = [agg]
        if agg == ['']:
            agg = []
        if self.xagg =='':
            agg.append('merge_f')
        else:
            agg.extend(['merge_f',self.xagg])
        agg = list(set(agg))
        self.Index_decom_agg = agg
        return df.groupby(agg).apply(self.Index_decom)
    
    def fault_static_decom(self,g):
        df = pd.Series()
        df['merge_f'] = self.indexone(g,'merge_f')
        df['f_system'] = self.indexone(g,'f_system')
        df['f_wnum'] = g['f_wnum'].sum()
        return df
    
    def fault_static(self):
       ## 排名	故障名称	机组数	故障频次	故障占比	累计占比
        df1 = self.df.drop_duplicates('f_month')
        df = self.df.groupby('merge_f').apply(self.fault_static_decom)
        tmpdf = self.turnum[self.turnum['cor_ora_name'].isin(self.df['cor_ora_name'].drop_duplicates())]
        ydf = pd.DataFrame(np.repeat('y',len(df1)),columns = ['y'])
        self.f_month = ydf['y'].str.cat(df1['f_month'].str.replace('-',''))
        print u'数据筛选段为%d个月'%(len(self.f_month))
        tmpdf = tmpdf[self.f_month] 
        df['turnum'] =  tmpdf.sum().sum()
##        df['turnum']= self.df[['cor_ora_name','turnum','f_month']].groupby(['cor_ora_name','f_month']).apply(self.fault_converge_com).sum()
        df['FF'] = df['f_wnum'].astype(float)/df['turnum']
        df = df.sort_values('FF',ascending=False)
        df.index = range(len(df))
        df['rank'] = df.index+1
        df['f_propo'] = df['f_wnum'].astype(float)/df['f_wnum'].sum()
        t = lambda x:sum(df['f_propo'][:x+1])*100
        df['add _propo'] = map(t,range((len(df))))
        df.to_excel('result/Fault_static.xlsx',index = False)
        print 'Fault_static save successful'
        if self.topN == 0:
            df = df[self.topS:]
        else:
            df = df[self.topS:self.topN]
        return df['merge_f']

    def fault_converge_com(self,g):
        return self.indexone(g,'turnum')
                
    def fault_converge(self,g,con,line):
        df = pd.Series()
        if self.xagg!= '':
            if len(g)!=0:
                df[self.xagg] = self.indexone(g,self.xagg)
            else:
                df[self.xagg] = con
        if self.yagg!= '':
            if len(g)!=0:
                df[self.yagg] = self.indexone(g,self.yagg)
            else:
                df[self.yagg] = line
        ## tmp为X轴和Y轴字段，若demoncheck（算法2），则故障频次分母保持不变，否则按照分母为切片内的台月数
        tmp = {self.xagg:con,self.yagg:line}
        if "merge_f" in tmp.keys():
            tmp.pop("merge_f")
        if ""  in  tmp.keys():
            tmp.pop("")
        tmpdf = self.df
        if self.demoncheck == False:
##            print u'目前算法1，台月数按照切片后的数据计算'
            for i in tmp.keys():
                tmpdf = self.judgelist(tmpdf,i,tmp[i])
        else:
            print u'目前算法2，台月数按照总体数据计算'

        ## 故障月份如果作为X或者Y维度的选项，则需要按照单月计算FF的分母
        if "f_month" in tmp.keys():
##            print u'因为维度中含有故障月份，故按照单月计算台月数'
            tt =tmpdf['f_month'].drop_duplicates()
            ydf = pd.DataFrame(np.repeat('y',len(tt)),columns = ['y'])
            f_month = ydf['y'].str.cat(tt.str.replace('-',''))    
        else:
            f_month = self.f_month
        ## tmpdf 为为X轴和Y轴字段切片台月数计算df，若demoncheck（算法2），tmpdf为筛选总数据，即分母不随切片变化

        df['f_wnum'] = g['f_wnum'].sum()
        df['FDT'] = g['f_wtime'].sum()
##        df['FMDT'] = df['FDT']/g['turnum'].mean()
       
        ## 计算tmpdf的台月数，按照数据库中的turnum表来计算，避免因为某月某风场没有出现故障，而未计算该月的台月数
        tmpdf1 = self.turnum[self.turnum['cor_ora_name'].isin(tmpdf['cor_ora_name'].drop_duplicates())]
        tmpdf1 = tmpdf1[f_month]
        
        df['turnum'] =  tmpdf1.sum().sum()
        ######################
        
        df['FMDT'] = df['FDT']/df['turnum']

        
        try:
            df['FF'] = float(df['f_wnum'])/df['turnum']
        except:
            df['FF'] = np.nan
        return df


    def judgeagg(self,df,agg):
        if agg!= '':
            return list(df[agg].drop_duplicates())
        else:
            return['']
        
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

    def judgelist(self,df,agg,ax):
        # df 针对单元素或数组的筛选
        if not isinstance(ax,list):
            return df[df[agg] == ax]
        else:
            return df[df[agg].isin(ax)]

    def comple(self,a,b):
        # a元素 对应b个数补全
        if not isinstance(a,list) or (isinstance(a,list) and len(a) == 1):
            if isinstance(a,list):
                t = a[0]
            else:
                t = a
            return np.repeat(t,b)
        else:
            return a
    
    def ansys_plot(self,axis,turnum ,ylabel = [],yagg = '',lines = [[]],plotbz = True,figuretype = 'line',legend = [],converge = [[]],xlegend = [],xagg = '',target = [],xlabel = '',title = '',figname = 'test.png',Flist = [],topS = 0,topN = 20,PSIL_value = None,demoncheck = False):
        print u'开启故障计算'
        self.turnum = turnum
        convergebz = (converge == [[]])
        self.demoncheck = demoncheck
        self.topS = topS
        self.topN = topN
        self.yagg = yagg
        self.xagg = xagg
        '''
        fault_static 得到故障名称的统计总表
        Flist得到要统计的故障区域
        axis [] 决定了Y轴数量，主要针对指标最多2个 ([计算指标名称]) ********
        ylabel [] 决定了单个指标绘制的Y轴标签，数量与axis保存一致
        yagg  决定曲线绘制标识 单值 ********
        lines [[]] 决定了单个指标绘制的线数，主要针对yagg标识聚合
        figuretype  决定了单个指标绘制的图片类型
        Flist 决定了要筛选的故障名称，默认为空，则按照topS = 0,topN = 20 截取前FF靠前的20个故障
        PSIL_value 为PSIL起始基准值，默认是数组FF第一个值
        demoncheck 为针对特殊字段，FF的分母是否区别对待
        ================    ===============================
            character           description
            ================    ===============================
 |      kind : str
 |          - 'line' : line plot (default)
 |          - 'bar' : vertical bar plot
 |          - 'barh' : horizontal bar plot
 |          - 'hist' : histogram
 |          - 'box' : boxplot
 |          - 'kde' : Kernel Density Estimation plot
 |          - 'density' : same as 'kde'
 |          - 'area' : area plot
 |          - 'pie' : pie plot
 |          - 'scatter' : scatter plot
 |          - 'hexbin' : hexbin plot
            ================    ===============================
        
        legend []  决定了单个指标绘制的图例，数量与lines保存一致
        converge [[]] X轴的聚合，决定了X方向的点数，主要针对xagg标识聚合
        xlabel X轴方向标签
        title 图片名称
        '''
        if Flist == []:
            Flist = self.fault_static()
##        print u'筛选故障个数%d'%len(Flist) 
        df = self.df[self.df['merge_f'].isin(Flist)]
##        self.fault_stadf = df
        # df为 筛选topX后的df数据，对此进行各维度灵活分析
        ax,axN = self.judgeaxis(axis)
        if ylabel ==[]:
            ylabel = ax
        elif not isinstance(ylabel,list):
            ylabel = [ylabel]
        if lines == [[]]:
            lines = self.judgeagg(df,yagg)
        if legend ==[]:
            for i in lines:
                if not isinstance(i,list):
                    legend.append(i)
                else:
                    legend.append(i[0])
        if axN == -1:
            print u'指标过多，无法计算'
        else:
            ylabel = self.comple(ylabel,axN)
            relist = []
            relist3 = pd.DataFrame()
            if axN == 2:
                relist2 = []
            for figindex,line in enumerate(lines):
                if yagg != '':
                    tmp = self.judgelist(df,yagg,line)
                else:
                    tmp = df
                if convergebz:
                    converge = self.judgeagg(tmp,xagg)
                data = pd.DataFrame()
                # xagg,yagg 若为'' 则judgeagg返回['']，数据只循环一次，不会出现数据重复
                for con in converge:
                    if xagg != '':
                        datat =self.judgelist(tmp,xagg,con)
                    else:
                        datat = tmp
                    # datat 为经过XY维度切割后计算的最小单元
                    df1 = self.fault_converge(datat,con,line)
                    data = data.append(df1,ignore_index = True)
                data = self.PSIL(data,PSIL_value)
                print u'分析数据中间键如下：'
##                print data
                if xlegend ==[]:
                    data.index = data[xagg]
                else:
                    data.index = xlegend  
                # data 含有FF,F_wnum,PSIL三个指标,用于绘图
                relist.append(data[ax[0]])
                relist3 = relist3.append(data)
                if axN == 2:
                    relist2.append(data[ax[1]])
            # 应该是先设置index ，然后横向拼接，空出则是nan，然后整体绘图，就会保证不会漏点
            # 根据 N 的值绘制双轴还是单轴，根据 bar等关键因素绘制柱状或是线图
            redf1 = pd.DataFrame(relist,index = legend).T
            redf1 = redf1.fillna(0)
            redf2= pd.DataFrame()
            if axN == 2:
                redf2 = pd.DataFrame(relist2,index = legend).T
                redf2 = redf2.fillna(0)
            print u'结果计算完成'
            return {'redf2':relist3,'redf1':redf1}

        
