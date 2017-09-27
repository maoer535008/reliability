## coding=utf-8
import pandas as pd
import ConfigParser
import datetime

def read_df(path):
    try:
        dfs = pd.read_excel(path,sheetname=None)
        ## 整体故障为故障数据，整体可利用率为机组台数数据
        keyword1 = u'整体故障'
        keyword2 = u'可利用率'
        ## df1 为故障数据
        for key in dfs:
            if keyword1 in key:
                df1 = dfs[key]
        ## df2 为机组台数数据
        for key in dfs:
            if keyword2 in key:
                df2 = dfs[key]
        ## 保留所有不为空的数据
        df1 = df1.dropna(how = "all")
        df2 = df2.dropna(how = "all")
        if u'年月' in df1.columns:
            df1.loc[:,u'故障月份'] = df1[u'年月'].astype(str).map(lambda x:datetime.datetime.strptime(x,"%Y%m"))
        else:
            df1.loc[:,u'故障月份'] = pd.to_datetime(df1[u'截止时间'])
            df1.loc[:,u'故障月份'] = df1[u'故障月份'].map(lambda x:datetime.date(x.year,x.month,1))
        ## 删除容量类型为3000的数据   
        df1 = df1[df1[u'容量类型'].fillna(0).astype(int).astype(str)!='3000']
        ## 故障名称去空格
        df1[u'故障描述'] = df1[u'故障描述'].fillna("")
        df1[u'故障描述'] = df1[u'故障描述'].str.replace(" ","",100)
        df1.loc[:,u'故障码'] = df1[u'主故障串'].map(lambda x:unicode(x).split("[")[1][:-1] if '['in unicode(x) else unicode(x))
    except KeyError,e:
        print  u'文件中未找到字段： ',eval(unicode(e))
            
    return df1,df2

class etldata():
    def __init__(self,df,failure,protocol):
        self.df = df
        self.failure = failure
        self.protocol = protocol
        self.pre_cols = [u'协议号',u'故障码']

    def fail(self,s):
        tt = self.failure[(self.failure[self.pre_cols[0]]==s[self.pre_cols[0]])&(self.failure[self.pre_cols[1]]==s[self.pre_cols[1]])]
        if len(tt)!=0:
            return tt.iat[0,2]
        else:
            return ''
        
    def cov_fail_g(self,s,gz):
        d = s.to_frame()
        d.columns = [self.pre_cols[0]]
        d[self.pre_cols[1]] = gz
        tmp =  d.apply(self.fail,axis=1)
        tmp =  tmp[tmp!='']
        if len(tmp)==0:
            return ''
        else:
            return tmp.iat[0]
        
    def cov_fail(self,s):
        tag = ''
        for col in self.protocol.columns:
            tmp = self.protocol[self.protocol[col]==s[self.pre_cols[0]]]
            if len(tmp) == 0:
                continue
            else:
                for col1 in tmp.index:
                    re =  self.cov_fail_g(tmp.loc[col1,col:],s[self.pre_cols[1]])
                    if re == '':
                        continue
                    else:
                        return re
        return tag

    def fail_run(self):
        return self.df[self.pre_cols].apply(self.fail,axis=1)

    def cov_fail_run(self):
        return self.df[self.pre_cols].apply(self.cov_fail,axis=1)


def cov_falut_name(df1):
    try:
        pre_cols = [u'协议号',u'故障码']
        ## 根据协议号补全故障名称
        protocol = pd.read_excel("icon/protocol_table.xlsx")
        protocol = protocol.drop_duplicates()
        protocol = protocol.fillna(-1)
        protocol_columns = protocol.columns.tolist()
        protocol_columns.reverse()
        protocol = protocol[protocol_columns]
        for col in protocol.columns:
            try:
                protocol[col] = protocol[col].fillna(0).astype(int)
            except :
                pass
            protocol.loc[:,col] = protocol[col].astype(unicode)
            
        failure = pd.read_excel("icon/failure_table.xlsx")
        failure = failure.drop_duplicates()
        
        for i in pre_cols:
            try:
                failure.loc[:,i] = failure[i].fillna(0).astype(int)
            except :
                pass
            failure.loc[:,i] = failure[i].astype(unicode)
        res = {}
        dfnull = df1[df1[u'故障描述']=='']
        ## 基于协议号和故障码对空白故障描述进行补全
        if len(dfnull)==0:
            pass
        else:
            for i in pre_cols:
                try:
                    dfnull.loc[:,i] = dfnull[i].fillna(0).astype(int)
                except :
                    pass                    
                dfnull.loc[:,i] = dfnull[i].astype(unicode)
            etl = etldata(dfnull,failure,protocol)
            df1.loc[dfnull.index,u'故障描述'] =etl.fail_run()
            dfnull1 = df1[df1[u'故障描述']=='']                 
            ## 基于历史协议号和故障码对空白故障描述进行补全
            if len(dfnull1)==0:
                pass
            else:
                etl = etldata(dfnull1,failure,protocol)
                dfnull1.loc[:,u'故障描述'] =  etl.cov_fail_run()
                if len(dfnull1[dfnull1[u'故障描述']!=''])!=0:
                    res['new']=  dfnull1[dfnull1[u'故障描述']!='']     
                else:
                    dfnull1.loc[dfnull1[dfnull1[u'故障描述']==''].index,u'故障描述'] = u'数据异常'                
                    res['error'] =  dfnull1[dfnull1[u'故障描述']==u'数据异常'][pre_cols]
                    
                df1.loc[dfnull1.index,u'故障描述'] = dfnull1[u'故障描述']
        res['df1'] = df1
        res['failure'] = failure
    except KeyError,e:
        print  u'文件中未找到字段： ',eval(unicode(e))
            
    return res


def increase_fault(df1):
    try:
        ## 新增字段
        ## 故障描述	修正名称	技术合并	系统 （1-4）
        Locate_fault = pd.read_excel("icon/Locate_fault.xlsx")    
        ## iu1 2u1.. 转换 成 变流器
        Locate_fault = Locate_fault.dropna(how = "all")
        Locate_fault = Locate_fault.drop_duplicates(u'故障描述')
        tmp  = pd.merge(df1,Locate_fault,on = u'故障描述',how = 'left')
        x =tmp[u'故障描述'].str.slice(0,3).str.lower()
        y = x[x.isin(['1u1','1u2','1u3','2u1','2u2','2u3','3u1','3u2','3u3'])]
        tmp.loc[y.index,u'修正名称'] = tmp.loc[y.index,u'故障描述']
        tmp.loc[y.index,u'技术合并'] = u'变流器紧急停机'
        tmp.loc[y.index,u'系统'] = u'变流'

        ## ORACLE的项目名称	容量类型	ORACLE的项目名称修正	变桨系统类型修正	变流系统类型修正	特殊项目	项目编号	确定调试完成时间 （2-6）
        Locate_project = pd.read_excel("icon/Locate_project.xlsx" )
        Locate_project = Locate_project.dropna(how = "all")
        Locate_project = Locate_project.drop_duplicates([u'ORACLE的项目名称',u'容量类型'])
        tmp1 = pd.merge(tmp,Locate_project,on =[u'ORACLE的项目名称',u'容量类型'],how = "left")

        ## 针对特殊风场进行校正
        ## name	风机名称	ORACLE的项目名称修正 （2-1）
        Special_fram = pd.read_excel("icon/Special_fram.xlsx" )
        Special_fram = Special_fram.dropna(how = "all")
        Special_fram = Special_fram.drop_duplicates(['name',u'风机名称'])
        Special_fram = Special_fram.astype(unicode)

        tmp2 = tmp1[tmp1[u'ORACLE的项目名称修正'].isin(Special_fram['name'])][[u'ORACLE的项目名称修正',u'风机名称']]
        tmp2.columns = ['name',u'风机名称']
        tmp2 = tmp2.astype(unicode)
        tmp3 = pd.merge(tmp2,Special_fram,on =['name',u'风机名称'],how='left')
        tmp3.index = tmp2.index
        tmp1.loc[tmp2.index,u"ORACLE的项目名称修正"] = tmp3[u"ORACLE的项目名称修正"]

        tmp1 = tmp1.rename(index = str,columns = {u'故障描述（备份）':u'故障描述（原始备份）',u'故障时长':u'故障时间小时'})
        tmp1.loc[:,u'非技术因素影响维修'] = ''
        tmp1.loc[:,u'非技术因素影响故障'] = ''
        tmp1.loc[:,u'主要原因'] = ''
        tmp1.loc[:,u'原因描述'] = ''

        ## 字段标准顺序 icon/icon/standard
        cf = ConfigParser.SafeConfigParser()
        cf.read('icon/init.ini')
        standard = dict(cf.items('standard'))
        standard_cols = standard['cols'].decode("gb2312").split(",")
        df1 = tmp1[standard_cols]
    except KeyError,e:
        print  u'文件中未找到字段： ',eval(unicode(e))
            
    return df1,Locate_project,Special_fram


def increase_turnum(df2,Locate_project,Special_fram):
    try:
        ## 基于可利用率数据计算 机组台月数
        df2 = df2.rename(index = str,columns = {u'ORACLE项目名称':u'ORACLE的项目名称'})
        tmp = pd.merge(df2[[u'ORACLE的项目名称',u'容量类型',u'风机名称']],Locate_project[[u'ORACLE的项目名称',u'容量类型',u'ORACLE的项目名称修正']],on =[u'ORACLE的项目名称',u'容量类型'],how = "left")
        tmp = tmp.rename(index = str,columns = {u'ORACLE的项目名称':'name'})
        tmp2 = tmp[tmp[u'name'].isin(Special_fram['name'])][[u'name',u'风机名称']]
        tmp2 = tmp2.astype(unicode)
        tmp3 = pd.merge(tmp2,Special_fram,on =['name',u'风机名称'],how='left')
        tmp3.index = tmp2.index
        tmp.loc[tmp2.index,u"ORACLE的项目名称修正"] = tmp3[u"ORACLE的项目名称修正"]
    except KeyError,e:
        print  u'文件中未找到字段： ',eval(unicode(e))
            
    return tmp


def cov_turnum(df1,tmp):
    try:
        month = "y"+df1.loc[df1.index[0],u'故障月份'].strftime("%Y%m")
        print "nothon: ",len(tmp)
        tmp1 = tmp.groupby([u'ORACLE的项目名称修正',u'容量类型']).apply(len)
        tmp1 = tmp1.reset_index(level=[0,1]).rename(index = str,columns = {0:month})
        tmp2 = df1.groupby([u'ORACLE的项目名称修正',u'容量类型']).apply(len)
        tmp2 = tmp2.reset_index(level=[0,1]).rename(index = str,columns = {0:month})
        common = tmp1[tmp1[u'ORACLE的项目名称修正'].isin(tmp2[u'ORACLE的项目名称修正'])][u'ORACLE的项目名称修正'].values
        ## common 是两者共同项目名称
        ## tmp1 不属于common的部分 如果存在 则代表 存在无故障数据，需要logging
        tmp3 = tmp1[~tmp1[u'ORACLE的项目名称修正'].isin(common)][[u'ORACLE的项目名称修正',u'容量类型']].drop_duplicates()
        tmp4 = tmp2[~tmp2[u'ORACLE的项目名称修正'].isin(common)][[u'ORACLE的项目名称修正',u'容量类型']].drop_duplicates()
    except KeyError,e:
        print  u'文件中未找到字段： ',eval(unicode(e))
            
    return tmp1,tmp2,tmp3,tmp4,common



