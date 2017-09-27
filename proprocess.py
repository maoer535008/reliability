#coding=utf-8
from PyQt4 import QtCore, QtGui, uic
from PyQt4.QtGui import *
from PyQt4.QtCore import *
import os,sys,string,datetime,time
import pandas as pd
import core,copy,ConfigParser
import pyperclip
import gc
import init2db
import init2redb
import numpy as np

def resource_path(relative_path):
    if hasattr(sys, "_MEIPASS"):
        base_path = sys._MEIPASS
    else:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

qtCreatorFile = resource_path( "proprocess.ui" )
Ui_MainWindow, QtBaseClass = uic.loadUiType(qtCreatorFile)


class DBdlg(QDialog):
    def __init__(self,parent=None):
        super(DBdlg,self).__init__(parent)  
    def setup(self,redf1,redf2):
        self.setWindowTitle(u'数据导入窗口')
        self.redf1 = redf1
        self.redf2 = redf2
        layout0=QGridLayout()
        check1 = QRadioButton(u'计算数据')
        self.check2 = QRadioButton(u'导入文件')
        check1.setChecked(True)
        layout0.addWidget(check1,0,0)
        layout0.addWidget(self.check2,0,1)
        ## 隐藏控件
        self.layout00=QGridLayout()
        self.L1 = QLabel(u'故障数据')
        self.path1 = QLineEdit()
        self.button1 = QToolButton()
        self.button1.setObjectName('path1')
        self.layout00.addWidget(self.L1,0,0)
        self.layout00.addWidget(self.path1,0,1)
        self.layout00.addWidget(self.button1,0,2)
        self.L2 = QLabel(u'机组台月数')
        self.path2 = QLineEdit()
        self.button2 = QToolButton()
        self.button2.setObjectName('path2')
        self.layout00.addWidget(self.L2,1,0)
        self.layout00.addWidget(self.path2,1,1)
        self.layout00.addWidget(self.button2,1,2)
        
        layout=QVBoxLayout()
        self.checks=[]
        qcheck1 = QCheckBox(u'厂级数据库')
        qcheck2 = QCheckBox(u'故障级数据库')
        qcheck3 = QCheckBox(u'重复故障级数据库')
        qcheck1.setChecked(True)
        qcheck2.setChecked(True)
        qcheck3.setChecked(True)
        self.checks.extend([qcheck1,qcheck2,qcheck3])
        layout.addWidget(qcheck1)
        layout.addWidget(qcheck2)
        layout.addWidget(qcheck3)
        self.button = QtGui.QPushButton(u'确认',self)
        layout1=QGridLayout()
        Lable1 = QLabel(u'数据库地址')
        self.host = QLineEdit()
        self.host.setText(u'10.76.13.97')
        layout1.addWidget(Lable1,0,0)
        layout1.addWidget(self.host,0,1)
        layout2 = QVBoxLayout()
        layout2.addLayout(layout0)
        layout2.addLayout(self.layout00)
        layout2.addLayout(layout)
        layout2.addLayout(layout1)
        layout2.addWidget(self.button)
        self.setLayout(layout2)
        QtCore.QObject.connect(self.button,QtCore.SIGNAL('clicked()'), self.getps)
        QtCore.QObject.connect(check1,QtCore.SIGNAL('clicked()'), self.showchoose)
        QtCore.QObject.connect(self.check2,QtCore.SIGNAL('clicked()'), self.showchoose)
        QtCore.QObject.connect(self.button1,QtCore.SIGNAL('clicked()'), self.choosefile)
        QtCore.QObject.connect(self.button2,QtCore.SIGNAL('clicked()'), self.choosefile)
        self.showchoose()
        self.show()

    def getps(self):
        reply = QMessageBox.question(self,u"确认消息框",  
                                    u"是否确认导入数据？",  
                                    QMessageBox.Ok|QMessageBox.Cancel,  
                                    QMessageBox.Ok)
        self.ops={}
        if reply==QMessageBox.Ok:
            self.ops['checks'] = [i.isChecked() for i in self.checks]
            self.ops['host'] = unicode(self.host.text())
            if self.check2.isChecked():
                try:
                    df1 = pd.read_excel(unicode(self.path1.text()))
                    df2 = pd.read_excel(unicode(self.path2.text()))
                    self.ops['redf1'] = df1
                    df1 = df1.replace("",np.nan)
                    df2 = df2.rename(index = str,columns = {u'ORACLE的项目名称修正':'cor_ora_name',u'容量类型':u'cor_cap_type'})
                    self.ops['redf2'] = df2
                    df2 = df2.replace("",np.nan)
                except Exception,e:
                    print unicode(e)
            else:
                self.ops['redf1'] = self.redf1
                self.redf2 = self.redf2.rename(index = str,columns = {u'ORACLE的项目名称修正':'cor_ora_name',u'容量类型':u'cor_cap_type'})
                self.ops['redf2'] = self.redf2
        self.close()
        
    def showchoose(self):
        self.L1.setVisible(self.check2.isChecked())
        self.L2.setVisible(self.check2.isChecked())
        self.path1.setVisible(self.check2.isChecked())
        self.path2.setVisible(self.check2.isChecked())
        self.button1.setVisible(self.check2.isChecked())
        self.button2.setVisible(self.check2.isChecked())

    def choosefile(self):
        tmpDir = QtGui.QFileDialog.getOpenFileName()
        if(len(tmpDir) > 0):
            if self.sender().objectName() == 'path1':
                self.path1.setText(unicode(tmpDir))
            else:
                self.path2.setText(unicode(tmpDir))


class AddaggDlg(QDialog):
    def __init__(self,parent=None):
        super(AddaggDlg,self).__init__(parent)  
    def setup(self,df):
        self.setWindowTitle(u'选择风场')
        self.df = df
        layout=QVBoxLayout()
        self.list = []
        for i,j in enumerate(df):
            numcheck = QCheckBox(j)
            numcheck.setChecked(True)
            self.list.append(numcheck)
            layout.addWidget(numcheck)
        self.button = QtGui.QPushButton(u'确认',self)
        QtCore.QObject.connect(self.button,QtCore.SIGNAL('clicked()'), self.checklist)
        layout.addWidget(self.button)
        self.setLayout(layout)
        self.show()

    def checklist(self):
        self.addlist = self.df[[i.isChecked() for i in self.list]]
        

class redb(QtCore.QThread):
     trigger = QtCore.pyqtSignal(type('')) 
     def __init__(self, parent=None):
         super(redb, self).__init__(parent)
         
     def setup(self,ops):
         self.ops = ops
         
     def run(self):
         if 'redf1' not in self.ops.keys():
             self.trigger.emit(u"请先计算数据或导入外部数据")
         elif (len(self.ops['redf1'])==0) | (len(self.ops['redf2'])==0):
             self.trigger.emit(u"故障数据或机组台月数为空")
         else:
             self.trigger.emit(u"启动原始数据导入")
             init2db.do_main(self.ops)
             self.trigger.emit(u"原始数据导入完成,启动三级数据库导入")
             init2redb.do_main(self.ops)
             self.trigger.emit(u"三级数据库导入完成")
             
             

class readdf(QtCore.QThread):
     trigger = QtCore.pyqtSignal(type({})) 
     def __init__(self, parent=None):
         super(readdf, self).__init__(parent)
         
     def setup(self,path):
         self.path = path
         
     def run(self):
         df1,df2 = core.read_df(self.path)
         self.trigger.emit({"df1":df1,"df2":df2})

class savedf(QtCore.QThread):
     trigger = QtCore.pyqtSignal(type(u"")) 
     def __init__(self, parent=None):
         super(savedf, self).__init__(parent)
         
     def setup(self,dfs):
         self.dfs = dfs
         
     def run(self):
         self.trigger.emit(u'正在执行结果导出，请稍等')
         for key in self.dfs.keys():
             self.dfs[key].to_excel(key,index=False)
         self.trigger.emit(u'导出结果成功')


class cov_falut(QtCore.QThread):
     trigger = QtCore.pyqtSignal(type({})) 
     def __init__(self, parent=None):
         super(cov_falut, self).__init__(parent)
         
     def setup(self,df):
         self.df = df
         
     def run(self):
         res= core.cov_falut_name(self.df)
         self.trigger.emit(res)
         
class increase_fault(QtCore.QThread):
     trigger = QtCore.pyqtSignal(type({})) 
     def __init__(self, parent=None):
         super(increase_fault, self).__init__(parent)
         
     def setup(self,df):
         self.df = df
         
     def run(self):
         df1,Locate_project,Special_fram= core.increase_fault(self.df)
         self.trigger.emit({"df1":df1,"Locate_project":Locate_project,"Special_fram":Special_fram})

class increase_turnum(QtCore.QThread):
     trigger = QtCore.pyqtSignal(type({})) 
     def __init__(self, parent=None):
         super(increase_turnum, self).__init__(parent)
         
     def setup(self,df,df1,df2):
         self.df = df
         self.df1 = df1
         self.df2 = df2
         
     def run(self):
         df2= core.increase_turnum(self.df,self.df1,self.df2)
         self.trigger.emit({"df2":df2})

class cov_turnum(QtCore.QThread):
     trigger = QtCore.pyqtSignal(type({})) 
     def __init__(self, parent=None):
         super(cov_turnum, self).__init__(parent)
         
     def setup(self,df1,df2):
         self.df1 = df1
         self.df2 =  df2
         
     def run(self):
         t1,t2,re1,re2,common= core.cov_turnum(self.df1,self.df2)
         self.trigger.emit({"re1":re1,"re2":re2,'t1':t1,'t2':t2,"common":common})
         
     
class MyApp(QtGui.QMainWindow, Ui_MainWindow):
    def __init__( self ):
        QtGui.QMainWindow.__init__( self )
        Ui_MainWindow.__init__( self )      
        self .setupUi( self )
        self.threads = []
        self.redf1 = pd.DataFrame()
        self.redf2 = pd.DataFrame()
        QtCore.QObject.connect(self.choosepath,QtCore.SIGNAL('clicked()'), self.openfile)
        QtCore.QObject.connect(self.reload_button,QtCore.SIGNAL('clicked()'), self.reloaddf)
        QtCore.QObject.connect(self.sure_button,QtCore.SIGNAL('clicked()'), self.do_main)
        QtCore.QObject.connect(self.export_button,QtCore.SIGNAL('clicked()'), self.export_result)
        QtCore.QObject.connect(self.data2db,QtCore.SIGNAL('clicked()'), self.todb)
        self.logname = datetime.datetime.now().strftime("log/%Y%m%d_%H%M%S_logging.txt")

    def todb(self):
        self.putform = DBdlg()
        self.putform.setup(self.redf1,self.redf2)        
        self.putform.button.clicked.connect(self.result2db)

    def result2db(self):
        if len(self.putform.ops.keys())!=0:
            thread = redb()
            thread.setup(self.putform.ops)
            thread.trigger.connect(self.update_text)
            thread.start()
            self.threads.append(thread)
        
    def export_result(self):
        try:
            try:
                t1 = self.t1
                tmpDir = QtGui.QFileDialog.getExistingDirectory()
                if(len(tmpDir) > 0):
                    name1,ok1 = QInputDialog.getText(self, u"输入文件名称",u"故障数据名称:",QLineEdit.Normal, u"data.xlsx")
                    name2,ok2 = QInputDialog.getText(self, u"输入文件名称",u"机组台月数文件名称:",QLineEdit.Normal, u"turnum.xlsx")
                    results = {}
                    if ok1:
                        results[os.path.join(unicode(tmpDir),unicode(name1))] = self.redf1
                    if ok2:
                        results[os.path.join(unicode(tmpDir),unicode(name2))] = self.redf2
                    thread = savedf()
                    thread.setup(results)
                    thread.trigger.connect(self.update_text)
                    thread.start()
                    self.threads.append(thread)
                    
            except Exception,e:
                self.update_text(unicode(e))
                self.update_text(u'请先执行分析操作')
        except Exception,e:
            self.update_text(u'导出结果失败')
            self.update_text(unicode(e))

    def logging(self,msg):
        if not os.path.exists("log"):
            os.makedirs("log")
        with open (self.logname,"a") as f:
            f.write(msg.encode("gb2312"))
            f.write("\n")
        f.close()

    def openfile(self):
        tmpDir = QtGui.QFileDialog.getOpenFileName()
        if(len(tmpDir) > 0):
            self.path.setText(unicode(tmpDir))

    def reloaddf(self):
        self.redf1 = pd.DataFrame()
        self.redf2 = pd.DataFrame([],columns = [u'ORACLE的项目名称修正',u'容量类型'])
        self.currentindex = 0
        self.df1 = self.df1s[self.months[self.currentindex]]
        self.df2 = self.df2s[self.months[self.currentindex]]
        self.update_text1(u'重载数据成功')
        self.update_text1("******************************")
        self.update_text(u'进入故障描述修正阶段') 
        self.month_ansys()

    def getdf(self,dfs):
        self.redf1 = pd.DataFrame()
        self.redf2 = pd.DataFrame([],columns = [u'ORACLE的项目名称修正',u'容量类型'])
        self.df1s = {}
        self.df2s = {}
        self.months = []
        dfs["df2"][u'截止时间'] = pd.to_datetime(dfs["df2"][u'截止时间']).dt.strftime("%Y%m")
        for month in dfs["df1"][u'故障月份'].drop_duplicates():
            self.months.append(month.strftime("%Y%m"))
            self.df1s[month.strftime("%Y%m")] = dfs["df1"][dfs["df1"][u'故障月份'] == month]
            self.df2s[month.strftime("%Y%m")] = dfs["df2"][dfs["df2"][u'截止时间'] == month.strftime("%Y%m")]
        self.currentindex = 0
        self.df1 = self.df1s[self.months[self.currentindex]]
        self.df2 = self.df2s[self.months[self.currentindex]]
        self.update_text(u'读取数据成功')
        self.update_text("******************************")
        self.month_ansys()

    def month_ansys(self):
        self.update_text("******************************")
        self.update_text(u"当前月份为： %s"%(self.months[self.currentindex]))
        self.update_text("******************************")
        self.update_text(u'进入故障描述修正阶段')
        thread = cov_falut()
        thread.setup(self.df1)
        thread.trigger.connect(self.cov_fal)
        thread.start()
        self.threads.append(thread)

    def update_text1(self,msg):
        self.message.append(msg)
        self.logging(msg)

    def cov_fal(self,dfs):
        self.df1 = dfs["df1"]
        failure = dfs['failure']
        if "new" in dfs.keys():
            self.update_text1( u'提示：发现新的协议号与故障的映射关系，是否更新配置文件')
            dfs['new'].to_clipboard()
            self.update_text1("-----------------------")
            self.update_text1(pyperclip.paste())
            self.update_text1("-----------------------")
            button=QMessageBox.question( self, u"提示框", u"发现新故障的映射关系\n是否更新配置文件？",QMessageBox.Ok|QMessageBox.Cancel, QMessageBox.Ok)
            if button==QMessageBox.Ok:
                failure.append(dfs['new'][[u'协议号',u'故障码',u'故障描述']].drop_duplicates())
                failure.to_excel("icon/failure_table.xlsx",index=False)
                self.update_text1(u"提示：协议号故障表配置文件已更新")
        if "error" in dfs.keys():
            self.update_text1( u"警告：协议号映射存在数据异常")
            dfs['error'].drop_duplicates().to_clipboard()
            self.update_text1("-----------------------")
            self.update_text1(pyperclip.paste())
            self.update_text1("-----------------------")
            
        self.update_text(u'故障描述修正阶段完成')    
        self.update_text("******************************")    
        self.update_text(u'进入增补新字段阶段')
        thread = increase_fault()
        thread.setup(self.df1)
        thread.trigger.connect(self.increase)
        thread.start()
        self.threads.append(thread)

    def increase(self,dfs):
        self.df1 = dfs["df1"]
        self.Locate_project = dfs['Locate_project']
        self.Special_fram = dfs['Special_fram']
        self.update_text(u'增补新字段阶段完成')
        self.update_text("******************************")
        if len(self.df1[self.df1[u'系统'].isnull()])!=0:
            ## 1to4存在问题
            self.update_text1( u'异常：存在故障描述匹配异常')
            logging2 =  self.df1[self.df1[u'系统'].isnull()][u'故障描述']
            logging2.drop_duplicates().to_clipboard()
            self.update_text1("-----------------------")
            self.update_text1(pyperclip.paste())
            self.update_text1("-----------------------")
            
        if len(self.df1[self.df1[u'ORACLE的项目名称修正'].isnull()])!=0:
            ## 2to6存在问题
            self.update_text( u'异常：存在ORACLE的项目名称匹配异常')
            logging3 = self.df1[self.df1[u'ORACLE的项目名称修正'].isnull()][[u'ORACLE的项目名称',u'容量类型',u'风机名称']]
            logging3.drop_duplicates().to_clipboard()
            self.update_text1("-----------------------")
            self.update_text1(pyperclip.paste())
            self.update_text1("-----------------------")
            
        self.update_text(u'进入机组台月数计算阶段')
        thread = increase_turnum()
        thread.setup(self.df2,self.Locate_project,self.Special_fram)
        thread.trigger.connect(self.b_turnum)
        thread.start()
        self.threads.append(thread)
    
    
    def b_turnum(self,dfs):
        self.df2 = dfs["df2"]
        self.update_text(u'机组台月数计算完毕')
        self.update_text("******************************")
        self.update_text(u'进入双向风场台月数矫正阶段')
        thread = cov_turnum()
        thread.setup(self.df1,self.df2)
        thread.trigger.connect(self.c_turnum)
        thread.start()
        self.threads.append(thread)
        
    def turnum_g(self,x):
        import numpy as np
        tmp = x[~x.isnull()]
        if len(tmp)==0:
            return np.nan
        else:
            return tmp.iat[0]        

    def c_turnum(self,dfs):
        re1 = dfs["re1"]
        self.re2  = dfs["re2"]
        self.t1 = dfs["t1"]
        self.common = dfs['common']
        if len(re1)!=0:
            self.update_text1( u"警告：存在无故障风场台月数")
            self.putform = AddaggDlg()
            self.putform.setup(re1[u"ORACLE的项目名称修正"])
            self.putform.button.clicked.connect(self.c_turnum1)

    def c_turnum1(self):
        month = "y"+self.df1.loc[self.df1.index[0],u'故障月份'].strftime("%Y%m")
        self.t1 = self.t1[(self.t1[u"ORACLE的项目名称修正"].isin(self.common))|(self.t1[u"ORACLE的项目名称修正"].isin(self.putform.addlist))] ## 筛选机组台月数
        self.putform.close()
        trouble_free = pd.read_excel("icon/trouble_free.xlsx")
        tmp = trouble_free[trouble_free[u"ORACLE的项目名称修正"].isin(self.putform.addlist)].fillna("")
        tmp.loc[:,[u'故障月份',u'开始时间',u'截止时间',u'故障月份']] = self.df1.loc[self.df1.index[0],[u'故障月份',u'开始时间',u'截止时间',u'故障月份']].values
        ttime = self.df1.loc[self.df1.index[0],u'故障月份']
        tmp.loc[:,[u'故障开始时间', u'故障截至时间',u'故障时间小时']] = [ttime,ttime+datetime.timedelta(seconds =120),round(2.0/60,2)]
        tmp[[u"ORACLE的项目名称修正",u'容量类型']].to_clipboard()
        self.update_text1("-----------------------")
        self.update_text1(pyperclip.paste())
        self.update_text1("-----------------------")
        self.df1 = self.df1.append(tmp) ## 增补故障数据
        cf = ConfigParser.SafeConfigParser()
        cf.read('icon/init.ini')
        standard = dict(cf.items('standard'))
        standard_cols = standard['cols'].decode("gb2312").split(",")
        self.df1 = self.df1.reindex(columns=standard_cols)
        if len(tmp)!=len(self.putform.addlist):
            self.update_text( u"异常：存在ORACLE的项目名称查无配置信息")
            self.putform.addlist[~self.putform.addlist.isin(tmp[u"ORACLE的项目名称修正"])].to_frame().to_clipboard()
            self.update_text1("-----------------------")
            self.update_text1(pyperclip.paste())
            self.update_text1("-----------------------")
            
        if len(self.re2)!=0:
            self.update_text( u"警告：有故障数据单缺少风场台月数")
            from re2sql import Mysql
            x = Mysql(host = "10.76.13.97",sqltype = 'postgre')
            turnum = x.showtdf("turnum")
            turnum['cor_ora_name'] = turnum['cor_ora_name'].map(lambda x:x.decode("utf-8"))
            x.conn.close()
            y = turnum[turnum['cor_ora_name'].isin(self.re2[u'ORACLE的项目名称修正'])]
            tmp = y[turnum.columns.drop(['cor_ora_name','cor_cap_type']).sort_values(ascending=False)].apply(self.turnum_g,axis=1)
            tmp1 = y[y.index.isin(tmp.index)][['cor_ora_name','cor_cap_type']]
            tmp1['cor_cap_type'] = tmp1['cor_cap_type'].astype(int).astype(str)
            tmp1 = tmp1.rename(index = str,columns = {u'cor_ora_name':u'ORACLE的项目名称修正',u'cor_cap_type':u'容量类型'})
            tmp1.loc[:,month] = tmp.values
            self.re2 = pd.merge(self.re2,tmp1,on=[u'ORACLE的项目名称修正',u'容量类型'],how = "left")
            x= self.re2[self.re2[month].isnull()]
            self.t1 = self.t1.append(self.re2) ## 增补机组台月数
            if len(x)!=0:
                x.to_clipboard()
                self.update_text1( u"异常：存在有故障数据，且查询历史风场台月数仍然无结果的风场")
                self.update_text1("-----------------------")
                self.update_text1(pyperclip.paste())
                self.update_text1("-----------------------")
                
        self.update_text(u'双向风场台月数矫正阶段完成')
        self.update_text("******************************")
        ## 结果增加
        self.redf1 = self.redf1.append(self.df1)
        self.redf2 = pd.merge(self.redf2,self.t1,on =[u'ORACLE的项目名称修正',u'容量类型'],how = "outer")
        self.currentindex +=1
        
        if self.currentindex!=len(self.months):
            self.df1 = self.df1s[self.months[self.currentindex]]
            self.df2 = self.df2s[self.months[self.currentindex]]
            self.update_text("******************************")
            self.month_ansys()
        else:
            for col in [u'故障开始时间',u'故障截至时间',u'故障维护时间']:
                self.redf1.loc[:,col] = pd.to_datetime(self.redf1[col])
            for col in [u'调试完成时间',u'开始时间',u'截止时间',u'故障月份',u'确定调试完成时间']:
                self.redf1.loc[:,col] = pd.to_datetime(self.redf1[col]).dt.date
                
            self.redf1.index = range(len(self.redf1))
            self.redf2.index = range(len(self.redf2))
##            self.redf1 = self.redf1.fillna("")
##            self.redf2 = self.redf2.fillna("")
            self.update_text(u'数据前处理完成')

    def do_main(self):
        path = unicode(self.path.text())
        self.update_text(u'正在读取%s'%(os.path.basename(path)))
        self.threads = []
        self.redf1 = pd.DataFrame()
        self.redf2 = pd.DataFrame()
        gc.collect()
        thread = readdf()
        thread.setup(path)
        thread.trigger.connect(self.getdf)
        thread.start()
        self.threads.append(thread)        

    def update_text(self,msg):
        self.message.append(msg)


if __name__ == "__main__" :
    app = QtGui.QApplication(sys.argv)
    window = MyApp()
    window.show()
    sys.exit(app.exec_())


