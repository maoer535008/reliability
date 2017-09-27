#coding=utf-8
import sqlite3
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

class Mysql():
    def __init__(self,dbname ='smart',sqltype = '',user='root', password='root', host='127.0.0.1', port='5432'):
        self.perkey = {}
        if sqltype =='':
            self.conn = sqlite3.connect(dbname)
        elif sqltype == 'postgre':
            self.engine = create_engine("postgresql://%s:%s@%s/%s"%(user,password,host,dbname),encoding='utf-8')
            self.conn =psycopg2.connect( user=user, password=password, host=host, port=port,database =dbname)
        else:
            self.conn = None
        self.sqltype = sqltype
        self.cur = self.conn.cursor()
        
    def creattable(self,tablename,col,perkey = None):
        # col 为字段 字典，key为字段名称，value是 字段类型
        if perkey!= None:
            Psql = ',PRIMARY KEY (%s)' % (perkey)
        else:
            Psql =''
            
        try:
            sql = ''.join([i+" "+col[i]+',' for i in col.keys()])[:-1]
            creat_sql = '''CREATE TABLE %s (%s%s) ''' % (tablename,sql,Psql)
            self.cur.execute(creat_sql)
            self.commit()
            print u'数据表创建成功'
            self.perkey[tablename] = perkey
        except Exception,e:
            print unicode(e)
            self.perkey[tablename] = perkey
            self.conn.rollback()
            
    def commit(self):
        self.conn.commit()

    def exec_(self,sql):
        try:
            self.cur.execute(sql)
            self.commit()
            rows = self.cur.fetchall()
            return rows
        except Exception,e:
            print unicode(e)
            self.conn.rollback()

    def deletetabel(self,tablename):
        sql = 'DROP TABLE %s' % tablename
        try:
            self.cur.execute(sql)
            self.commit()
            print u'删除表成功'
        except Exception,e:
            print unicode(e)
            self.conn.rollback()

    def insertdata(self,tablename,data,kind = False):
        col = ','.join(["%s" for i in range(len(data.keys()))])
        col1 = ','.join(["'%s'" for i in range(len(data.keys()))])
        sql = '''INSERT INTO %s (%s) values (%s)''' % (tablename,col,col1)
        data1 = list(data.keys())
        data1.extend(list(data.values()))
        insert_sql = sql % tuple(data1)
        try:
            self.cur.execute(insert_sql)
            self.commit()
            print u'数据插入成功'
        except Exception,e:
            if kind:
                con = {}
                con[self.perkey[tablename]] = data[self.perkey[tablename]]
                self.updatedata(tablename,data,con)
            else:
                print unicode(e)
                self.conn.rollback()
                
    def showperkey(self,tablename):
        if tablename not in self.perkey.keys():
            for i in self.showschema(tablename):
                if i[-1] ==1:
                    self.perkey[tablename] = i[1].encode('gb2312')
                    return i[1].encode('gb2312')
                    break
            return None
        else:
            return self.perkey[tablename]
          
    def insertdatas(self,tablename,datas,columns = None):
        # data为showtable 所示的rows 是 list类型
        if columns == None:
            if self.sqltype =='':
                N = 1
            elif self.sqltype == 'postgre':
                N =2
            tt = [i[N] for i in self.showschema(tablename)]
        else:
            tt = columns
        col = ','.join(["%s" for i in range(len(tt))]) % (tuple(tt))
        tt1 = lambda x:"("+','.join(["'%s'" for i in range(len(x))]) % (tuple(x))+")"
        col1 = ','.join(map(tt1,datas))
        insertdatas_sql = "INSERT INTO %s (%s) values %s;" % (tablename,col,col1)
        try:
            self.cur.execute(insertdatas_sql)
            self.commit()
            print u'插入多行数据成功 %d行 ' % (len(datas)) 
        except Exception,e:
            print unicode(e)
            self.conn.rollback()

    def altertable(self,tablename,col):
        # col为要插入的新字段，比较col和tablename中的schma，取差值，col为字典{}，key为字段名，value为字段类型
        if self.sqltype =='':
            N = 1
        else:
            N = 2 
        schema = [i[N] for i in self.showschema(tablename)]
        cha = list(set(col.keys()).difference(set(schema)))
        tmp = []
        for i in cha:
            tmp.append("add %s %s"%(i,str(col[i])))                    
        altersql = 'alter table %s '%tablename +','.join(tmp)+';'
        try:
            self.cur.execute(altersql)
            self.commit()
            print u'%s新增%d个字段成功' % (tablename,len(cha)) 
        except Exception,e:
            print unicode(e)
            self.conn.rollback()              
        
    def showtable(self,tablename):
        select_sql ="select * from %s" % (tablename)
        try:
            self.cur.execute(select_sql)
            rows = self.cur.fetchall()
            return rows
        except Exception,e:
            print unicode(e)
            self.conn.rollback()
    
    def listtables(self):
        if self.sqltype =='':
            sql = "SELECT name FROM sqlite_master WHERE type='table' order by name"
        elif self.sqltype == 'postgre':
            sql = "select tablename from pg_tables where schemaname='public'"
        self.cur.execute(sql)
        rows = [i[0] for i in self.cur.fetchall()]
        return rows

    def showtdf(self,tablename,columns = [],number=-1):
        try:
            if self.sqltype =='':
                N = 1
                df = pd.DataFrame(self.showtable(tablename),columns = [i[N] for i in self.showschema(tablename)])
            elif self.sqltype == 'postgre':
                if columns == []:
                    coltext = "*"
                elif isinstance(columns,type([])):
                    coltext = ','.join(columns)
                else:
                    coltext = columns
                    
                if number==-1:
                    numtext = ''
                elif number>=0:
                    numtext = "LIMIT %d"%number
                else:
                    numtext = "LIMIT 5"  
                df = pd.read_sql_query('select %s from %s %s'%(coltext,tablename,numtext),con=self.engine)
        except Exception,e:
            print unicode(e)
            df = pd.DataFrame()
        return df
    
    def showschema(self,tablename):
        if self.sqltype =='':
            sql ="PRAGMA table_info(%s)" % (tablename)
        elif self.sqltype == 'postgre':
            sql = '''SELECT col_description(a.attrelid,a.attnum) as comment,format_type(a.atttypid,a.atttypmod) as type,a.attname as name, a.attnotnull as notnull  
                    FROM pg_class as c,pg_attribute as a  
                    where c.relname = '%s' and a.attrelid = c.oid and a.attnum>0  ''' % (tablename.lower())
        try:
            self.cur.execute(sql)
            rows = self.cur.fetchall()
            return rows
        except Exception,e:
            print unicode(e)
            self.conn.rollback()

    def deletedata(self,tablename,data):
        # data 为字段 字典，key为字段名称，value是数据
        col = ' and '.join(["%s = '%s'" for i in range(len(data.keys()))])
        sql = '''delete from %s where %s'''% (tablename,col)
        tmp = []
        [tmp.extend(i) for i in data.items()]
        delete_sql = sql % tuple(tmp)
        try:
            self.cur.execute(delete_sql)
            self.commit()
            print u'删除数据成功' 
        except Exception,e:
            print unicode(e)
            self.conn.rollback()

    def updatedata(self,tablename,data,condition):
        # data  字典，key为字段名称，value是数据 要更新的数据
        # condition 字典，key为字段名称，value是数据 更新条件
        chemainit = dict([(i[2],i[1].split(' ')[0]) for i in self.showschema(tablename)])
        col = []
        for i in range(len(data.keys())):
            if chemainit[data.items()[i][0]]=='integer' or chemainit[data.items()[i][0]]=='double':
                tmp = "%s = %s"
            else:
                tmp = "%s = '%s'"
            col.append(tmp)
        col = ' , '.join(col)
        col1 = []
        for i in range(len(condition.keys())):
            if chemainit[condition.items()[i][0]]=='integer' or chemainit[condition.items()[i][0]]=='double':
                tmp = "%s = %s"
            else:
                tmp = "%s = '%s'"
            col1.append(tmp)
        col1 = ' and '.join(col1)
        sql = '''UPDATE %s SET %s WHERE %s'''% (tablename,col,col1)                                       
        tmp = []
        [tmp.extend(i) for i in data.items()]
        [tmp.extend(i) for i in condition.items()]
        updata_sql = sql % tuple(tmp)
        try:
            self.cur.execute(updata_sql)
            self.commit()
            print u'更新数据成功' 
        except Exception,e:
            print unicode(e)
            self.conn.rollback()

    def colcom(self,g):
        if self.sqltype == 'postgre':
            return "("+','.join(["'%s'" for i in range(len(g.index))]) % (tuple(g.tolist()))+")"
        else:
            return ' UNION ALL SELECT '+','.join(["'%s'" for i in range(len(g.index))]) % (tuple(g.tolist()))
            
        
    def insertdf(self,tablename,df):
        col = ','.join(["%s" for i in range(len(df.columns))]) % (tuple(df.columns.tolist()))
        if self.sqltype != 'postgre':
            col1 = ''.join(df.apply(self.colcom,axis = 1))
            col1 = col1.replace("UNION ALL SELECT",'SELECT',1)
            insertdf_sql = "INSERT INTO %s (%s) %s;" % (tablename,col,col1)
        else:
            col1 = ','.join(df.apply(self.colcom,axis = 1))
            insertdf_sql = "INSERT INTO %s (%s) values %s;" % (tablename,col,col1)
        try:
            self.cur.execute(insertdf_sql)
            self.commit()
            print u'插入数据表成功 %d行 ' % (len(df)) 
        except Exception,e:
            print unicode(e)
            self.conn.rollback()
