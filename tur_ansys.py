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
        self.Index = ['MTBF_time','MTBF_num','FF_F','FF_turnum','MTBR_time','MTBR_num','MTTR_time','MTTR_num','MRT_time','MRT_num','DT_time','MDT_num','MTBI_time','MTBI_num']
                          
    def indexone(self,s,a):
        return s.loc[s.index[0], a]

    def Index_decom_group(self,g):
        df = pd.Series()
        for i in self.Index_decom_agg:
            df[i] = self.indexone(g,i)
        for i in ['month_hour','turnum','f_wtime','f_wnum','f_rtime','f_rnum','rtime']:
            g[i] = g[i].astype(float)
        if g['turnum'].mean()!= 0:
            df['MTBF_time'] = (g['month_hour'].mean())*(g['turnum'].mean()) - sum(g['f_wtime'])    # MTBF分子项
            df['MTBF_num'] = g['f_wnum'].sum()                                                      # MTBF分母项
            df['FF_F'] = g[ 'f_wnum'].sum()                                                   # 频次分子项
            df['FF_turnum'] = g['turnum'].mean()                                              # 频次分母项
            df['MTBR_time'] =(g['month_hour'].mean())*(g['turnum'].mean()) - sum(g['f_rtime'])      # MTBR分子项
            df['MTBR_num'] = g['f_rnum'].sum()                                                       # MTBR分母项
            df['MTTR_time'] = g['f_wtime'].sum()                                                     # MTTR分子项
            df['MTTR_num'] = g['f_wnum'].sum()                                                       # MTTR分母项
            df['MRT_time'] = g['rtime'].sum()                                                     # MRT分子项
            df['MRT_num'] = g['f_rnum'].sum()                                                       # MRT分母项
            df['DT_time'] = g['f_wtime'].sum()                                                     # DT分子项
            df['MDT_num'] = g['turnum'].mean()                                                     # MDT分母项
            df['MTBI_time'] =(g['month_hour'].mean())*(g['turnum'].mean())                         # MTBI分子项
            df['MTBI_num'] =  g['f_mnum'].sum()                                                    # MTBI分母项 
        else:
            df1 = pd.Series([0 for i in range(len(self.Index))],index = self.Index )
            df = df.append(df1)
        return df
            
    def Index_decom(self,g):
        # groupby之后计算指标分解参数，返回参数字段 10个,并且保留agg标识(self.Index_decom_agg)
        '''
        MTBF（平均无故障运行时间）
        MTBF=（统计范围机组台数*统计时间内日历小时数-故障总小时数）/故障次数
        a)	统计“故障数据”涉及的机组台数；
        b)	统计故障次数；
        c)	统计故障时长；
        d)	统计日历天数；
        e)	基于以上数据进行计算。
        FF故障频次
        故障频次=总故障次数/总风机台数，单位为（台*次/月）
        a)	统计“故障数据”涉及的机组台数；
        b)	统计故障次数；
        c)	基于以上数据进行计算。
        MTBR（平均修复间隔时间）
        MTBR=（统计范围机组台数*统计时间内日历小时数-修复类故障总小时数）/修复类故障次数
        a)	修复类故障指数据中包括故障维护时间记录的故障；
        b)	计算方法等同于MTBF，统计修复类故障次数和停机时间时所有不同。
        MTBI()
        MTBI = 统计总时间/检修次数(维修时间有值并且一天内最多只记一次)
        MTTR（平均故障修复时间）
        MTTR=故障停机总时间/故障次数
        在不同的故障维修级别进行统计：
        a)	对于全部故障，统计其故障次数和停机时间，基于公式进行计算，结果计为MTTR；
        b)	对于有故障维护时间的故障，统计其故障次数和停机时间，基于公式进行计算，结果计为MTTR（CAT1～CAT4）；
        MRT（平均故障修复工作时间或平均维修时间）
        MRT=故障停机过程中工作时间/有现场工作的故障次数
        a)	挑选有故障维护时间的故障；
        b)	统计故障次数；
        c)	统计现场工作时间，现场工作时间的起止分别为故障维护时间和故障截至时间；
        d)	基于公式计算结果。
        '''
        ## MTBF
        df = g.groupby(['cor_ora_name','f_month']).apply(self.Index_decom_group)
        df = df[df['MTBF_time']>=0]
        return df
    
    def groupstatic(self,df =None,agg = ''):
        if not df:
            df = self.df
        if not isinstance(agg,list):
            agg = [agg]
        if agg == ['']:
            agg = []
        if self.xagg =='':
            agg.extend(['cor_ora_name','f_month'])
        else:
            agg.extend(['cor_ora_name','f_month',self.xagg])
        agg = list(set(agg))
        self.Index_decom_agg = agg
        return df.groupby(agg).apply(self.Index_decom)
                
    def Index_agg(self,g):
        # 引用self.Index 进行指标聚合计算
        # 返回 字段名称为  MTBF,MTI...
        df = pd.Series()
        if self.xagg!= '':
            df[self.xagg] = g[self.xagg]
        if self.yagg!= '':
            df[self.yagg] = g[self.yagg]
        zb = ['MTBF','FF','MTBR','MTTR','MRT','FN','DT','MDT','MTBI']
        fj = [['MTBF_time','MTBF_num'],['FF_F','FF_turnum'],['MTBR_time','MTBR_num'],['MTTR_time','MTTR_num'],['MRT_time','MRT_num'],['MTBF_num',1],['DT_time',1],['DT_time','MDT_num'],['MTBI_time','MTBI_num']]
        for i in range(len(zb)):
            if isinstance(fj[i][1],str):
                num = g[fj[i][1]]
            else:
                num = fj[i][1]
            if num!=0:
                df[zb[i]] = round(float(g[fj[i][0]])/num,2)
            else:
                df[zb[i]] = np.nan
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
    
    def ansys_plot(self,axis ,ylabel = [],yagg = '',lines = [[]],plotbz = True,figuretype = 'line',legend = [],converge = [[]],xlegend = [],xagg = '',target = [],xlabel = u'月份',title = '',figname = 'test.png'):
        print u'开启计算'
        if xagg =="" and yagg == "":
            xagg= "f_month"
            converge = [self.judgeagg(self.df,xagg)]
            xlegend= [u'total']
        if xagg =="" and yagg != "":
            xagg= "f_month"
                    
        self.yagg = yagg
        self.xagg = xagg
        convergebz = (converge == [[]])
        '''
        df 为 groupstatic得到的df
        MTBF=（每月风机台数*统计时间内日历小时数-故障总小时数）/故障总数
        每月故障频次=每月总故障次数/总风机台数，便能够得到该月故障频次
        MTBI=统计周期内小时数*统计机组台数/检修次数，得到该月的MTBI
        MTTR=统计范围内故障造成的统计时间总和/故障次数，得到该月的MTTR
        df 是10个字段的 指标参数中间键
        axis [] 决定了Y轴数量，主要针对指标最多2个 ([计算指标名称]) ********
        ylabel [] 决定了单个指标绘制的Y轴标签，数量与axis保存一致
        yagg  决定曲线绘制标识 单值 ********
        lines [[]] 决定了单个指标绘制的线数，主要针对yagg标识聚合
        figuretype  决定了单个指标绘制的图片类型

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
        df = self.groupstatic(agg = yagg)
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
            # df字段为xagg，yagg，10个指标分解字段(可以在def1中 作为self.Index= '10个指标分解字段')
            ylabel = self.comple(ylabel,axN)
            redf = pd.DataFrame()
            indf = pd.DataFrame()
            relist = []
            if axN == 2:
                relist2 = []
            for figindex,line in enumerate(lines):
                if yagg != '':
                    tmp = self.judgelist(df,yagg,line)
                else:
                    tmp = df
                redf = redf.append(tmp)
                if convergebz:
                    converge = self.judgeagg(tmp,xagg)
                datatt = []
                for con in converge:
                    if xagg != '':
                        datat =self.judgelist(tmp,xagg,con)
                    else:
                        datat = tmp
                    xt = datat[self.Index].sum()
                    if xagg!='':
                        xt[self.xagg] = con
                    if yagg!='':
                        xt[self.yagg] = line
                    datatt.append(xt)
                data = pd.DataFrame(datatt)
                df1 = data.apply(self.Index_agg,axis =1)
                df1[yagg] = legend[figindex]
                if xlegend ==[]:
                    df1.index = df1[xagg]
                else:
                    df1.index = xlegend
                indf = indf.append(df1)
                relist.append(df1[ax[0]])
                if axN == 2:
                    relist2.append(df1[ax[1]])
            # 应该是先设置index ，然后横向拼接，空出则是nan，然后整体绘图，就会保证不会漏点
            # 根据 N 的值绘制双轴还是单轴，根据 bar等关键因素绘制柱状或是线图
##            redf1 = pd.DataFrame(relist,index = lines).T
            redf1 = pd.DataFrame(relist,index = legend).T
            redf1 = redf1.fillna(0)
            redf2 = pd.DataFrame()
            if axN == 2:
##                redf2 = pd.DataFrame(relist2,index = lines).T
                redf2 = pd.DataFrame(relist2,index = legend).T
                redf2 = redf2.fillna(0)
            print u'结果计算完毕'
            return {'redf':redf,'indf':indf,'redf1':redf1,'redf2':redf2}
        
    def Warehouse(self,g):
        df1 = pd.Series()
        for i in range(len(self.xlist)-1):
            tmp = g[(g>self.xlist[i]) & (g<self.xlist[i+1])]
            if i == len(self.xlist)-2:
                le = '>'+str(self.xlist[i])
            else:
                le = str(self.xlist[i+1])
            df1[le] = len(tmp)
        return df1
    
    def concentrate_plot(self,axis ,ylabel = u'风场数量',yagg = '',lines = [[]],plotbz = True,figuretype = 'line',legend = [],xagg = '',xlist = range(0,5200,240),xlabel = u'MTBF',title = '',figname = 'test.png',percent=False):
        print u'开启百分比计算'
        re = self.ansys_plot(axis = axis,yagg=yagg,lines = lines,plotbz = False,xagg = xagg,legend = legend)
        d1 = re['redf']
        d2 = re['indf']
        self.xlist = xlist
        groups = d2.groupby(yagg)
        df1 = []
        legendbz = False
        if legend ==[]:
            legendbz = True 
        for tdf in groups:
            if legendbz:
                legend.append(tdf[0])
            df1.append(self.Warehouse(tdf[1][axis[0]]))
        df1 = pd.DataFrame(df1,index = legend).T
        if percent:
            df1 = df1/df1.sum()*100
        print u'结果计算完成'
        return {'redf':d1,'indf':df1}
