#!/usr/bin/python
# -*- coding: UTF-8 -*-
import json
import csv
import os
import sys
from PyQt4 import QtCore, QtGui, uic
from PyQt4.QtGui import *
from PyQt4.QtCore import *
import string,datetime,time
import pandas as pd
from re2sql import Mysql
from sqlalchemy import create_engine
import tur_ansys
import fau_ansys
from pylab import mpl
import pylab as pl
from Tkinter import *
import matplotlib.pyplot as plt
import json
import shutil
import ref_ansys
import FileDialog
import ConfigParser
import forecast
import proprocess

if getattr(sys, 'frozen', None):
     basedir = sys._MEIPASS
else:
     basedir = os.path.dirname(__file__)
     
qtCreatorFile = os.path.join(basedir, "main.ui" )
Ui_MainWindow, QtBaseClass = uic.loadUiType(qtCreatorFile)
########## 解决pandas中文标签显示#########
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False
########## 解决pandas中文标签显示######### 
       

class Exmain(QtCore.QThread):
    trigger = QtCore.pyqtSignal(str)
  
    def __init__(self, parent=None):
        super(Exmain, self).__init__(parent)

    def setup(self,init,fiterdict,tablename,tableschma,filename,filename1):
        self.filename =filename
        self.filename1 =filename1
        self.stime = time.time()
        engine = create_engine("postgresql://%s:%s@%s/%s"%(init['username'],init['password'],init['serverhost'],init['servername']),encoding='utf-8')
        schema = pd.read_sql_query('select * from %s'%tableschma,con=engine)
        self.colname = ','.join(["%s" for i in range(len(schema))])%tuple(schema['head'])
        comcol = list(set(schema['head'])& set(fiterdict.keys()))
        x = dict(zip(schema['head'],schema['type']))
        self.fiter ={}
        self.dtype = {}
        for i in comcol:
            self.fiter[i] = fiterdict[i]
            self.dtype[i] = x[i]
        self.engine = engine
        self.tablename = tablename
        self.chinese = dict(zip(schema['head'],schema['chinese']))
             
        
    def run(self):
        self.trigger.emit (u'发现%d 重合筛选条件字段'%len(self.fiter.keys()))
        tmp1 = []
        if len(self.fiter.keys())!=0:
            for col in self.fiter.keys():
                if self.dtype[col].lower() not in ['integer','float']:
                    tmpstr = "'%s'"
                else:
                    tmpstr = "%s"
                tmp = "("+','.join([tmpstr for i in range(len(self.fiter[col]))])+")"
                tmp1.append(col+" in "+tmp%tuple(self.fiter[col]))
            sql = " and ".join(tmp1)
            self.trigger.emit (u'数据正在导出中，请稍等')
            self.df = pd.read_sql_query('select %s from %s where %s'%(self.colname,self.tablename,sql),con=self.engine)
            self.df.columns = pd.Series(self.df.columns).map(self.chinese)
            
            self.df.to_excel("result/%s"%self.filename,index = False)
            self.trigger.emit (u'导出数据表%s完成，共计%d行，存储于result文件夹下，花费%0.2fs'%(self.filename,len(self.df),time.time()-self.stime))
        else:
            self.trigger.emit (u'筛选条件在数据表中 查无字段，请确认筛选条件字段')
            
        turnum = pd.read_sql_query('select * from turnum',con=self.engine)
        tmp = {}
        for col in turnum.columns:
             if "y2" in col:
                  tmp[col] = u'%s年%s月'%(col[1:5],col[5:])
             else:
                  tmp[col] = col
        tmp["cor_ora_name"] = u'ORACLE的项目名称修正'
        tmp["cor_cap_type"] = u"容量类型"
        turnum.columns = pd.Series(turnum.columns).map(tmp)
        turnum.to_excel("result/%s"%self.filename1,index = False)
        self.trigger.emit (u'导出%s完毕，共计%d行，存储于result文件夹下'%(self.filename1,len(turnum)))

class Pmain(QtCore.QThread):
    trigger = QtCore.pyqtSignal(dict)
  
    def __init__(self, parent=None):
        super(Pmain, self).__init__(parent)

    def setup(self,ansys,df,ops):
        self.bz = (ansys == tur_ansys)
        self.bz1 = (ansys == ref_ansys)
        self.y = ansys.mdata_ansys(df)
        self.ops =ops
    def run(self):
        ops =self.ops
        if ops['percent']:
            re = self.y.concentrate_plot(axis= ops['axis'] ,ylabel =ops['ylabel'],yagg = ops['yagg'],lines = ops['lines'],plotbz =ops['plotbz'],figuretype = ops['figuretype'],legend = ops['legend'],xagg = ops['xagg'],xlist = range(0,5200,240),xlabel = ops['xlabel'],title = ops['title'],figname = ops['figname'],percent=ops['percent'])
            self.trigger.emit (re)    
        else:
            if self.bz:
                re= self.y.ansys_plot(axis =ops['axis'],ylabel = ops['ylabel'],yagg = ops['yagg'],lines = ops['lines'],plotbz = ops['plotbz'],figuretype = ops['figuretype'],legend = ops['legend'],converge = ops['converge'],xlegend = ops['xlegend'],xagg = ops['xagg'],target =ops['target'],xlabel = ops['xlabel'],title = ops['title'],figname = ops['figname'])
                self.trigger.emit (re)
            elif self.bz1:
                re= self.y.ansys_plot(turnum = ops['turnum'],repeattime =ops['repeattime'],demoncheck = ops['demoncheck'],axis =ops['axis'],ylabel = ops['ylabel'],yagg = ops['yagg'],lines = ops['lines'],figuretype = ops['figuretype'],legend = ops['legend'],converge = ops['converge'],xlegend = ops['xlegend'],xagg = ops['xagg'],target =ops['target'],xlabel = ops['xlabel'],title = ops['title'],figname = ops['figname'])
                self.trigger.emit (re)                
            else:
                re= self.y.ansys_plot(axis =ops['axis'],ylabel = ops['ylabel'],yagg = ops['yagg'],lines = ops['lines'],plotbz = ops['plotbz'],figuretype = ops['figuretype'],legend = ops['legend'],converge = ops['converge'],xlegend = ops['xlegend'],xagg = ops['xagg'],target =ops['target'],xlabel = ops['xlabel'],title = ops['title'],figname = ops['figname'],Flist = [],topS = ops['topS'],topN = ops['topN'],PSIL_value = ops['PSIL_value'],demoncheck = ops['demoncheck'],turnum = ops['turnum'])
                self.trigger.emit (re) 

class AddaggDlg(QDialog):
    def __init__(self,parent=None):
        super(AddaggDlg,self).__init__(parent)  
    def setup(self,agg,df):
        self.x= df
        self.x =self.x.sort_values()
        self.tpye = df.dtype
        self.setWindowTitle(agg)
        label1=QLabel(u"区间名称")
        label2=QLabel(u"关键字查询")
        self.aggname = QLineEdit()
        self.keyword = QLineEdit()
        self.listcols = QListView()
        self.listcols.setSelectionMode(2)
        self.listcols.setModel(MyModel(df))
        layout0 = QHBoxLayout()
        self.snum = QSpinBox()
        self.enum = QSpinBox()
        self.snum.setMinimum (0)
        self.snum.setMaximum (10000)
        self.enum.setMinimum (0)
        self.enum.setMaximum (10000)
        self.snum.setValue(0)
        self.enum.setValue(10000)
        self.numcheck = QCheckBox()
        self.numcheck.setChecked(False)
        self.snum.setEnabled(False)
        self.enum.setEnabled(False)
        self.numcheck.clicked.connect(self.numcheckd)
        self.snum.editingFinished.connect(self.numcheckd)
        self.enum.editingFinished.connect(self.numcheckd)
        self.keyword.textEdited.connect(self.keyselect)
        layout0.addWidget(QLabel(u"启动数值筛选"))
        layout0.addWidget(self.numcheck)
        layout0.addWidget(QLabel(u" 最小值"))
        layout0.addWidget(self.snum)
        layout0.addWidget(QLabel(u"最大值"))
        layout0.addWidget(self.enum)
        layout=QGridLayout()  
        layout.addWidget(label1,0,0)
        layout.addWidget(self.aggname,0,1)
        layout.addWidget(label2,1,0)
        layout.addWidget(self.keyword,1,1)
        self.button = QtGui.QPushButton(u'确认',self)
        QtCore.QObject.connect(self.button,QtCore.SIGNAL('clicked()'), self.getCurrentIndex)
        layout1 = QVBoxLayout()
        layout1.addLayout(layout)
        layout1.addLayout(layout0)
        layout1.addWidget(self.listcols)
        layout1.addWidget(self.button)
        self.setLayout(layout1)
        self.keyselect()
        self.show()

    def numcheckd(self):
        if self.tpye != object:
            self.snum.setEnabled(self.numcheck.isChecked())
            self.enum.setEnabled(self.numcheck.isChecked())
            if self.numcheck.isChecked():
                self.x = self.x.astype(float)
                self.listcols.setModel(MyModel(self.x[(self.x>=self.snum.value())&(self.x<=self.enum.value())].astype(str)))
        else:
            self.numcheck.setChecked(False)

    def transfordata(self,data):
        if self.tpye!= object:
            return float(data)
        else:
            return data

    def getCurrentIndex(self):
        self.dictx = {unicode(self.aggname.text()):[self.transfordata(unicode(i.data().toString())) for i in self.listcols.selectedIndexes()]}

    def keyselect(self):
        if self.tpye != object:
            self.x = self.x.astype(str)
        tt = self.x[~(self.x.str.find(unicode(self.keyword.text()))==-1)]
        self.listcols.setModel(MyModel(tt))
        

def MyModel(df,head=None):
    if isinstance(df,pd.Series):
        model1 = QStandardItemModel()
        for i in df:
            item1 = QStandardItem(i)
            model1.appendRow(item1)
    else:
        if len(df)!=0:
            try:
                 model1 = QStandardItemModel(len(df),len(df[0]))
                 if head!=None:
                     model1.setHorizontalHeaderLabels(head)
                 for i,j in enumerate(df):
                     for ii,jj in enumerate(j):
                         model1.setData(model1.index(i, ii, QModelIndex()), unicode(jj))
            except Exception,e:
                 model1 = QStandardItemModel(len(df),len(df[0]))
                 if head!=None:
                     model1.setHorizontalHeaderLabels(head)
                 for i,j in enumerate(df):
                     for ii,jj in enumerate(j):
                         model1.setData(model1.index(i, ii, QModelIndex()), jj.decode("utf-8"))
        else:
            model1 = QStandardItemModel()
    return model1



class Userdiaglog(QDialog):
    def __init__(self,parent=None):
        super(Userdiaglog,self).__init__(parent)
        
    def setup(self,ops):
        self.ops = ops
        self.setWindowTitle(u"用户自定义功能模块")
        label1=QLabel(u"模块名称")
        label2=QLabel(u"模块备注")
        self.name = QLineEdit()
        self.remarks = QTextEdit()
        layout=QGridLayout()  
        layout.addWidget(label1,0,0)
        layout.addWidget(self.name,0,1)
        layout.addWidget(label2,1,0)
        layout.addWidget(self.remarks,1,1)
        self.button = QtGui.QPushButton(u'确认',self)
        layout1 = QVBoxLayout()
        layout1.addLayout(layout)
        layout1.addWidget(self.button)
        self.setLayout(layout1)
        self.show()


class Userselect(QDialog):
    def __init__(self,parent=None):
        super(Userselect,self).__init__(parent)
        
    def setup(self,path,remarks):
        self.wh = [400.0,300.0]
        self.setWindowTitle(u"用户功能模块")
        label1=QLabel(u"示例图片")
        label2=QLabel(u"")
        label2.setScaledContents(True)
        image = QImage()
        image.load(path)
        if not image.isNull():
            martix = QMatrix()  
            martix.scale(self.wh[0]/image.width(),self.wh[1]/image.height())
            image=image.transformed(martix)
            label2.setPixmap(QPixmap.fromImage(image))
            label2.resize(image.width(),image.height())
        layout=QGridLayout()  
        layout.addWidget(label1,0,0)
        layout.addWidget(label2,1,0)
        self.remarks = QTextEdit()
        self.remarks.setText(remarks)
        layout.addWidget(self.remarks,2,0)
        layout1=QGridLayout()  
        self.button = QtGui.QPushButton(u'调用模型参数',self)
        self.button1 = QtGui.QPushButton(u'取消',self)
        self.button1.clicked.connect(self.close)
        layout1.addWidget(self.button,0,0)
        layout1.addWidget(self.button1,0,1)
        layout2 = QVBoxLayout()
        layout2.addLayout(layout)
        layout2.addLayout(layout1)
        self.setLayout(layout2)
        self.show()


class Modify(QDialog):
    def __init__(self,parent=None):
        super(Modify,self).__init__(parent)
        
    def setup(self,dirname):
        self.wh = [400.0,300.0]
        self.dirname = dirname
        self.setWindowTitle(u"用户功能模块管理界面")
        dirname = self.dirname
        self.tabs = QTabWidget()     
        for index,mpath in enumerate(os.listdir(dirname)):
            data = json.loads(open(os.sep.join([dirname,mpath]),"r").read())
            x = QWidget()
            self.tabs.addTab(x, data['modelname'])
        layout0 = QGridLayout()
        label_1=QLabel(u"模型名称") 
        self.filename = QLineEdit()
        label=QLabel(u"图片路径")
        self.figpath = QLineEdit()
        self.figpath.setText(data['figname'])
        self.button0 = QtGui.QPushButton(u'更新图片',self)
        self.button0.clicked.connect(self.updatefig)
        layout0.addWidget(label_1,0,0)
        layout0.addWidget(self.filename,0,1)
        layout0.addWidget(label,0,2)
        layout0.addWidget(self.figpath,0,3)
        layout0.addWidget(self.button0,0,4)
        layout=QGridLayout()  
        label1=QLabel(u"示例图片")
        ## 图片载入及缩放
        self.label2=QLabel(u"")
        self.label2.setScaledContents(True)
        self.label2.resize(self.wh[0],self.wh[1])
        ## 图片载入及缩放
        self.remarks = QTextEdit()
        self.remarks.setText(data['remarks'])
        self.remarks.setObjectName("remarks%d"%index)
        layout.addWidget(label1,0,0)
        layout.addWidget(self.label2,1,0)
        layout.addWidget(self.remarks,2,0)           
        tlayout = QHBoxLayout()
        self.qbutton = QtGui.QPushButton(u'关闭窗口',self)
        self.qbutton.clicked.connect(self.close) 
        self.button = QtGui.QPushButton(u'修改模块',self)
        self.button.clicked.connect(self.updatemodel)
        self.button1 = QtGui.QPushButton(u'删除模块',self)
        self.button1.clicked.connect(self.deletemodel)
        tlayout.addWidget(self.button)
        tlayout.addWidget(self.button1)
        tlayout.addWidget(self.qbutton)
        layout2 = QVBoxLayout()
        layout2.addWidget(self.tabs)
        layout2.addLayout(layout0)
        layout2.addLayout(layout) 
        layout2.addLayout(tlayout)
        self.setLayout(layout2)
        self.tabs.currentChanged.connect(self.changetab) 
        self.changetab()
        self.show()

    def changetab(self):
        modelname = unicode(self.tabs.tabText(self.tabs.currentIndex()))
        filepath = os.path.join(self.dirname,modelname)
        data = json.loads(open(filepath,"r").read())
        self.figpath.setText(data['figname'])
        self.remarks.setText(data['remarks'])
        self.filename.setText(data['modelname'])
        self.update()

    def update(self):
        image = QImage()
        image.load(unicode(self.figpath.text()))
        if not image.isNull():
            martix = QMatrix()  
            martix.scale(self.wh[0]/image.width(),self.wh[1]/image.height())
            image=image.transformed(martix)
            self.label2.setPixmap(QPixmap.fromImage(image))
            self.label2.resize(image.width(),image.height())
        else:
            self.label2.setPixmap(QPixmap(unicode(self.figpath.text())))
            self.label2.resize(0,0)

    def updatefig(self):
        tmpDir = QtGui.QFileDialog.getOpenFileName() 
        if(len(tmpDir) > 0):
            self.figpath.setText(unicode(tmpDir))
            self.update()
        
    def updatemodel(self):
        modelname = unicode(self.tabs.tabText(self.tabs.currentIndex()))
        newmodel = unicode(self.filename.text())
        path = unicode(self.figpath.text())
        newpath = path.replace(os.path.dirname(path),"icon")
        remarks = unicode(self.remarks.toPlainText())
        filepath = os.path.join(self.dirname,modelname)
        try:
            shutil.copy(path,newpath)
        except:
            pass
        data = json.loads(open(filepath,"r").read())
        data['figname'] = newpath
        data['remarks'] =remarks
        data['modelname'] =newmodel
        with open(filepath,"w")as f:
            f.write(json.dumps(data))
        os.rename(filepath,os.path.join(self.dirname,newmodel))
        self.figpath.setText(newpath)
        self.tabs.setTabText(self.tabs.currentIndex(), newmodel)
        print u'修改模型成功'

    def deletemodel(self):
        reply = QMessageBox.question(self,u"确认消息框",  
                                    u"是否确认删除？",  
                                    QMessageBox.Ok|QMessageBox.Cancel,  
                                    QMessageBox.Ok)
        if reply==QMessageBox.Ok:
            modelname = unicode(self.tabs.tabText(self.tabs.currentIndex()))
            filepath = os.path.join(self.dirname,modelname)
            self.tabs.removeTab(self.tabs.currentIndex())
            os.remove(filepath)        
            print u'删除模型成功'


class FilterDlg(QDialog):
    def __init__(self,parent=None):
        super(FilterDlg,self).__init__(parent)  
    def setup(self,df,schma,dictx,dbtype):
        self.dbtype = dbtype
        self.setWindowTitle(u'数据表筛选')
        label1=QLabel(u"选择筛选字段")
        label2=QLabel(u"关键字查询")
        self.df =df
        self.dictx =dictx
        self.cols = QComboBox()
        self.keyword = QLineEdit()
        self.listcols = QListView()
        self.listcols.setSelectionMode(2)
        self.keyword.textEdited.connect(self.keyselect) 
        self.schma = schma
        ## 针对重复故障数据库，特定修改：隐藏次数及标识字段，数值型筛选
        if dbtype ==2:
            tmp = [i for i in self.schma['head'] if i.find("nf")!=0 ]
            self.cols.addItems(list(self.schma['chinese'][self.schma['head'].isin(tmp)]))
            self.cols.addItems([u'重复故障时间段',u'重复故障次数'])
        else:
            self.cols.addItems(self.schma['chinese'])  
        layout=QGridLayout()  
        layout.addWidget(label1,0,0)
        layout.addWidget(self.cols,0,1)
        layout.addWidget(label2,1,0)
        layout.addWidget(self.keyword,1,1)
        layout0 = QHBoxLayout()
        self.snum = QSpinBox()
        self.enum = QSpinBox()
        self.snum.setMinimum (0)
        self.snum.setMaximum (10000)
        self.enum.setMinimum (0)
        self.enum.setMaximum (10000)
        self.snum.setValue(0)
        self.enum.setValue(10000)
        self.numcheck = QCheckBox()
        self.numcheck.setChecked(False)
        self.snum.setEnabled(False)
        self.enum.setEnabled(False)
        self.numcheck.clicked.connect(self.numcheckd)
        self.cols.currentIndexChanged.connect(self.fitercols)
        self.snum.editingFinished.connect(self.numcheckd)
        self.enum.editingFinished.connect(self.numcheckd)
        layout0.addWidget(QLabel(u"启动数值筛选"))
        layout0.addWidget(self.numcheck)
        layout0.addWidget(QLabel(u" 最小值"))
        layout0.addWidget(self.snum)
        layout0.addWidget(QLabel(u"最大值"))
        layout0.addWidget(self.enum)
        self.button = QtGui.QPushButton(u'确认',self)
        QtCore.QObject.connect(self.button,QtCore.SIGNAL('clicked()'), self.getCurrentIndex)
        layout1 = QVBoxLayout()
        layout1.addLayout(layout)
        layout1.addLayout(layout0)
        layout1.addWidget(self.listcols)
        layout1.addWidget(self.button)
        self.setLayout(layout1)
        self.show()
        self.fitercols()

    def numcheckd(self):
        if self.tpye != object:
            self.snum.setEnabled(self.numcheck.isChecked())
            self.enum.setEnabled(self.numcheck.isChecked())
            if self.numcheck.isChecked():
                self.x = self.x.astype(float)
                self.listcols.setModel(MyModel(self.x[(self.x>=self.snum.value())&(self.x<=self.enum.value())].astype(str)))
        else:
            self.numcheck.setChecked(False)
            
            
    def transfordata(self,data):
        if self.tpye!= object:
            return float(data)
        else:
            return data
    
    def getCurrentIndex(self):
        if self.colname not in self.dictx.keys():
            self.dictx[self.colname] = [self.transfordata(unicode(i.data().toString())) for i in self.listcols.selectedIndexes()]
        else:
            self.dictx[self.colname].extend([self.transfordata(unicode(i.data().toString())) for i in self.listcols.selectedIndexes()])
            self.dictx[self.colname] = list(set(self.dictx[self.colname]))
        
    
    def keyselect(self):
        if self.x.dtype != object:
            self.x = self.x.astype(str)
        tt = self.x[~(self.x.str.find(unicode(self.keyword.text()))==-1)]
        self.listcols.setModel(MyModel(tt))
    
    def fitercols(self):
        tt =self.schma[self.schma['chinese'] == unicode(self.cols.currentText())]
        if len(tt)!=0:
            self.colname =tt.loc[tt.index[0],'head']
            self.x = self.df[self.colname].drop_duplicates()
            self.x = self.x.sort_values()
            self.tpye = self.x.dtype
            if self.x.dtype != object:
                self.x = self.x.astype(str)
            self.listcols.setModel(MyModel(self.x))
            self.numcheck.setChecked(False)
        else:
            tmp = [i for i in self.schma['head'] if i.find("nfm")==0 ]
            if unicode(self.cols.currentText()) == u'重复故障时间段':
                self.colname = u'重复故障时间段'
                self.x = pd.Series(tmp)
                self.x.sort_values()
                self.tpye  = object
                self.listcols.setModel(MyModel(self.x))
            elif unicode(self.cols.currentText()) == u'重复故障次数':                
                self.colname = u'重复故障次数'
                ttx = []
                for colname in tmp:
                    ttx.extend(list(self.df[colname].drop_duplicates()))
                ttx = list(set(ttx))
                ttx.sort()
                self.tpye  = float
                self.x = pd.Series(ttx).astype(str)
                self.listcols.setModel(MyModel(self.x))

class MyApp(QtGui.QMainWindow, Ui_MainWindow):
    def __init__( self ):
        QtGui.QMainWindow.__init__( self )
        Ui_MainWindow.__init__( self )      
        self .setupUi( self )
        self.tabs.tabBar().hide()
        self.dblistsdict={0:[['MTBF'],['FF'],['MTBR'],['MTBI'],['MTTR'],['MRT'],['DT'],['MDT']],1:[['FF'],['f_wnum'],['PSIL'],['FDT'],['FMDT']],2:[['Cff'],['Cfp'],['Cfef'],['Cfep'],['Cft']],3:[['Gf'],["Pbf"]]}
        self.dblistsinit = {}
        self.dblistsinit[0] = {"tablename":'tur_result',"tableschma":'turschema',"ansys":tur_ansys,"percentChecked":False,"percentEnabled":True,"PSIL_value":False,"topS":False,"topN":False,"demoncheckChecked":False,"demoncheckEnabled":False,"repeattime":False} 
        self.dblistsinit[1] = {"tablename":'fau_result',"tableschma":'fauschema',"ansys":fau_ansys,"percentChecked":False,"percentEnabled":False,"PSIL_value":True,"topS":True,"topN":True,"demoncheckChecked":False,"demoncheckEnabled":True,"repeattime":False}
        self.dblistsinit[2] = {"tablename":'ref_result',"tableschma":'refschema',"ansys":ref_ansys,"percentChecked":False,"percentEnabled":False,"PSIL_value":False,"topS":False,"topN":False,"demoncheckChecked":False,"demoncheckEnabled":True,"repeattime":True}
        self.cf = ConfigParser.ConfigParser()
        try:
            self.cf.read('icon/func.ini')
        except:
            pass
        self.tabs.setCurrentIndex(0)
        self.conn =None
        self.fiterdict = {}
        self.init()
        self.redfs=[]
        self.repeattime.setCurrentIndex(0)
        self.figuretype.addItems(['line','bar','barh','hist','box','kde','density','area','pie','scatter','hexbin'])
        self.demoncheck.setChecked(False)
        self.xcheckBox.setChecked(False)
        self.ycheckBox.setChecked(False)
        self.plotbz.setChecked(False)
        self.percent.setChecked(False)
        self.converge.setEnabled(False)
        self.lines.setEnabled(False)
        self.topS.setMinimum (0)
        self.topS.setMaximum (10000)
        self.topN.setMinimum (0)
        self.topN.setMaximum (10000)
        self.PSIL_value.setMinimum (-1000)
        self.PSIL_value.setMaximum (10000)
        self.topS.setValue (0)
        self.topN.setValue (1000)
        self.PSIL_value.setValue (-1.00)
        self.axiss.setSelectionMode(2)
        self.xagglist=[]
        self.yagglist=[]
        self.logo.setScaledContents(True)
        image = QImage()
        image.load(r'icon/logo.png')
        if not image.isNull():
            martix = QMatrix()  
            martix.scale(200.0/image.width(),180.0/image.height())
            image=image.transformed(martix)
            self.logo.setPixmap(QPixmap.fromImage(image))
            self.logo.resize(image.width(),image.height())
        self.xcheckBox.stateChanged.connect(self.changeagg)
        self.ycheckBox.stateChanged.connect(self.changeagg)
        ## 给listview添加右键快捷菜单
        self.popMenu = QtGui.QMenu()
        action1 = QtGui.QAction(u"删除", self,
                                 priority=QtGui.QAction.LowPriority,
                                 triggered=self.deletkey)
        self.popMenu.addAction(action1)
        self.fiterlist.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.popMenu1 = QtGui.QMenu()
        action1 = QtGui.QAction(u"增加区间", self,
                                 priority=QtGui.QAction.LowPriority,
                                 triggered=self.addnewagg)
        action2 = QtGui.QAction(u"删除该区", self,
                                 priority=QtGui.QAction.LowPriority,
                                 triggered=self.deleteagg)
        self.popMenu1.addAction(action1)
        self.popMenu1.addAction(action2)
        self.converge.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.lines.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        ##
        self.modellist.setRowCount(100)
        self.modellist.setColumnCount(1)
        self.modellist.setHorizontalHeaderLabels([u'模块名称'])
        self.dblists.addItems([u'机组级指标计算',u'故障级指标计算',u'重复故障指标计算',u'关键器件寿命'])
        QtCore.QObject.connect(self.gonext,QtCore.SIGNAL('clicked()'), self.selecttable)
        QtCore.QObject.connect(self.exportfile,QtCore.SIGNAL('clicked()'), self.savefiles)
        QtCore.QObject.connect(self.filtercol,QtCore.SIGNAL('clicked()'), self.filtercols)
        QtCore.QObject.connect(self.upto2,QtCore.SIGNAL('clicked()'), self.uptoac2)
        QtCore.QObject.connect(self.gonext1,QtCore.SIGNAL('clicked()'), self.filterdf)
        ## 寿命模块页面
        QtCore.QObject.connect(self.upto3,QtCore.SIGNAL('clicked()'), self.uptoac3)
        QtCore.QObject.connect(self.gonext3,QtCore.SIGNAL('clicked()'), self.goac3)
        QtCore.QObject.connect(self.upto4,QtCore.SIGNAL('clicked()'), self.uptoac4)
        
        QtCore.QObject.connect(self.plotbz,QtCore.SIGNAL('clicked()'), self.enableops)
        QtCore.QObject.connect(self.dblists,QtCore.SIGNAL('currentIndexChanged(int)'), self.changelistmodel) 
        self.listmodels.itemClicked.connect(self.changemodel)
        self.functionlist.itemClicked.connect(self.changefunc)
        QtCore.QObject.connect(self.tabWidget,QtCore.SIGNAL('currentChanged(int)'), self.changetabWidget)
        QtCore.QObject.connect(self.tabWidget1,QtCore.SIGNAL('currentChanged(int)'), self.changetabWidget)
        QtCore.QObject.connect(self.xagg,QtCore.SIGNAL('currentIndexChanged(int)'), self.initagg)
        QtCore.QObject.connect(self.yagg,QtCore.SIGNAL('currentIndexChanged(int)'), self.initagg)
        self.connect(self.fiterlist,QtCore.SIGNAL("customContextMenuRequested(const QPoint&)"),self.onButtonPopMenu)
        self.connect(self.converge,QtCore.SIGNAL("customContextMenuRequested(const QPoint&)"),self.onButtonPopMenu)
        self.connect(self.lines,QtCore.SIGNAL("customContextMenuRequested(const QPoint&)"),self.onButtonPopMenu)
        QtCore.QObject.connect(self.gonext2,QtCore.SIGNAL('clicked()'), self.mainansys)
        QtCore.QObject.connect(self.usercontrol,QtCore.SIGNAL('clicked()'), self.saveparameters)
        QtCore.QObject.connect(self.modify,QtCore.SIGNAL('clicked()'), self.modifymodel)
        QtCore.QObject.connect(self.exportdata,QtCore.SIGNAL('clicked()'), self.exportdb)
        QtCore.QObject.connect(self.exportdata1,QtCore.SIGNAL('clicked()'), self.exportdb)
        QtCore.QObject.connect(self.nhdtable, QtCore.SIGNAL('customContextMenuRequested (const QPoint&)'), self.nhdtablemenu)
        QtCore.QObject.connect(self.tableView_2, QtCore.SIGNAL('customContextMenuRequested (const QPoint&)'), self.nhdtablemenu)
        QtCore.QObject.connect(self.groupBox2, QtCore.SIGNAL('customContextMenuRequested (const QPoint&)'), self.groupBox2menu)
        QtCore.QObject.connect(self.groupBox3, QtCore.SIGNAL('customContextMenuRequested (const QPoint&)'), self.groupBox2menu)
        QtCore.QObject.connect(self.modellist2, QtCore.SIGNAL('customContextMenuRequested (const QPoint&)'), self.modellist2menu)
        QtCore.QObject.connect(self.predict_button, QtCore.SIGNAL('clicked()'), self.show_predict)
        QtCore.QObject.connect(self.processdata, QtCore.SIGNAL('clicked()'), self.show_pro)
        self.nhdtable.setModel(MyModel([]))
        self.tableView_2.setModel(MyModel([]))
        self.groupimagexdict = {'-3':"",'-2':"",'-1':"",'0':'','1':'','2':'','3':''}
        screen = QtGui.QDesktopWidget().screenGeometry()
        size = self.geometry()
        self.move((screen.width()-size.width())/2,(screen.height()-size.height())/2-40)
        self.enableops()
        self.changelistmodel()

    def show_predict(self):  
        if self.ansys ==tur_ansys:
             try:
                  df = self.redfs['indf']
                  bz = True
             except:
                  df = pd.DataFrame()
                  bz = False
             name,ok = QInputDialog.getText(self, u"指标筛选",u"指标名称:",QLineEdit.Normal, "MTBF")
             if ok:
                  if bz:
                       x = dict(zip(self.schma['head'],self.schma['chinese']))
                       indexs = list(df.columns[:list(df.columns).index("MTBF" )])
                       df = df[indexs+[unicode(name)]]
                       df = df.sort_index()
                       if len(indexs) ==2:
                            indexs.remove("f_month")
                            df1 = df.pivot_table(index = "f_month",columns =indexs[0]).T
                            df2 = df1.index.levels[1].to_series().to_frame()
                            df2.columns  = [x[indexs[0]]]
                            df1.index=df2.index
                            df = pd.merge(df2,df1,left_index=True,right_index=True)
                       else:
                            df = df[df.columns.drop(indexs[0])].T
                            df.index=[0]
                            df = pd.merge(pd.DataFrame([[u'值']],columns=[u'段']),df,left_index=True,right_index=True)
                  self.forecast = forecast.MyApp()
                  self.forecast.maindf = df
                  self.forecast.show()

    def show_pro(self):
        self.proprocess = proprocess.MyApp()
        self.proprocess.show()

    def changetabWidget(self):
        if type(self.sender()) == type(QTabWidget()):
             if self.sender().objectName() == "tabWidget":
                  self.imageindex = self.sender().currentIndex()
             else:
                  self.imageindex = self.sender().currentIndex()-3
             if self.imageindex<0:
                  tmp=self.preanalysisplot
             else:
                  tmp=self.tmpimage         
             self.input_image(tmp)

    def input_image(self,tmp):                  
        path = self.groupimagexdict[str(self.imageindex)]   
        tmp.setPixmap(QPixmap(path))
        tmp.setScaledContents(True)
        tmp.resize(tmp.geometry().width(),tmp.geometry().height())

    def modellist2menu(self,point):
        popMenu = QtGui.QMenu()
        tmp = QtGui.QAction(u'创建模块',self)
        self.connect(tmp,QtCore.SIGNAL('triggered()'),self.createmodel1)
        popMenu.addAction(tmp)
        popMenu.exec_(QtGui.QCursor.pos())

    def modelbutton2menu(self,point):
        self.currentmodelbutton = unicode(self.sender().text())
        popMenu = QtGui.QMenu()
        tmp = QtGui.QAction(u'修改模块',self)
        self.connect(tmp,QtCore.SIGNAL('triggered()'),self.modifymodel1)
        popMenu.addAction(tmp)
        tmp = QtGui.QAction(u'删除模块',self)
        self.connect(tmp,QtCore.SIGNAL('triggered()'),self.deletemodel1)
        popMenu.addAction(tmp)
        popMenu.exec_(QtGui.QCursor.pos())


    def createmodel1(self, name= None):
        ok = False
        if name == None:
             name,ok = QInputDialog.getText(self, u"输入模块名称",u"模块名称:",QLineEdit.Normal, "")
        if ok or name:
             model2 = {} 
             model2['name'] = unicode(name)
             model2['datarange'] = unicode(self.datarange.toPlainText())
             model2['datarange2'] = unicode(self.datarange2.toPlainText())
             model2['preanalysis'] = unicode(self.preanalysis.toPlainText())
             model2['groupBrowser4'] = unicode(self.groupBrowser4.toPlainText())
             tmp =  self.nhdtable.model()
             nhdtabledata = [] 
             for i in range(tmp.rowCount()):
                  tmpdata = []
                  for j in range(tmp.columnCount()):
                       tmpdata.append(unicode(tmp.data(tmp.index(i,j)).toString()))
                  nhdtabledata.append(tmpdata)
             model2['nhdtable'] = json.dumps(nhdtabledata)
             tableView_2 = []
             tmp =  self.tableView_2.model()
             for i in range(tmp.rowCount()):
                  tmpdata = []
                  for j in range(tmp.columnCount()):
                       tmpdata.append(unicode(tmp.data(tmp.index(i,j)).toString()))
                  tableView_2.append(tmpdata)          
             model2['tableView_2'] = json.dumps(tableView_2)
             
             if not os.path.exists("model/3/%s"%name):
                  os.makedirs("model/3/%s"%name)
             for key in self.groupimagexdict.keys():
                  oldpath = self.groupimagexdict[key]
                  if (oldpath!="") and os.path.exists(oldpath):
                       figtype= os.path.basename(oldpath).split(".")[-1]
                       newpath = "model/3/%s/%s.%s"%(name,key,figtype)
                       if oldpath!=newpath:
                            shutil.copy(oldpath,newpath)
                       self.groupimagexdict[key] = newpath
                            
             model2['paths'] = self.groupimagexdict
             f = open(u"model/3/%s/model"%name,"w")
             f.write(json.dumps(model2).encode("gb2312"))
             f.close()
             self.refreshmodellist2()
        
    def modifymodel1(self):
        filename,ok = QInputDialog.getText(self, u"提示框", u"确定修改模块？",QLineEdit.Normal,self.currentmodelbutton)
        if ok :
             if filename == self.currentmodelbutton:
                  self.createmodel1(self.currentmodelbutton)
                  os.rename("model/3/%s"%self.currentmodelbutton,"model/3/%s"%filename)
             else:
                  self.createmodel1(filename)
                  shutil.rmtree("model/3/%s"%self.currentmodelbutton)
                  
             self.refreshmodellist2()

    def deletemodel1(self):
        button=QMessageBox.question( self, u"提示框", u"确定删除模块？",QMessageBox.Ok|QMessageBox.Cancel, QMessageBox.Ok)
        if button==QMessageBox.Ok:    
             shutil.rmtree("model/3/%s"%self.currentmodelbutton)
             self.refreshmodellist2()
        
    def modelclicked2(self):
        button=QMessageBox.question( self, u"提示框", u"是否调用模块？",QMessageBox.Ok|QMessageBox.Cancel, QMessageBox.Ok)
        if button==QMessageBox.Ok:             
             key =  unicode(self.sender().text())
             tmp = self.modeldict2[key]
             self.groupimagexdict = tmp['paths']
             self.tableView_2.setModel(MyModel(json.loads(tmp['tableView_2'])))
             self.tableView_2.resizeColumnsToContents()             
             self.nhdtable.setModel(MyModel(json.loads(tmp['nhdtable'])))
             self.nhdtable.resizeColumnsToContents()
             self.datarange.setText(tmp['datarange'])
             self.datarange2.setText(tmp['datarange2'])
             self.preanalysis.setText(tmp['preanalysis'])
             self.groupBrowser4.setText(tmp['groupBrowser4'])
             self.imageindex = self.tabWidget1.currentIndex()-3
             self.input_image(self.preanalysisplot)  
             self.changetabWidget()
                  

    def refreshmodellist2(self):
        # 刷新modellist列表
        self.modeldict2 ={}
        self.modellist2.clear()
        dirname = "model/3"
        if os.path.exists(dirname):            
            models = os.listdir(dirname)
            for index in range(len(models)):
                self.modeldict2[models[index].decode("gb2312")] = json.loads(open(os.sep.join([dirname,models[index],'model']),"r").read())
                MyCombo = QtGui.QPushButton(models[index].decode("gb2312"),self)
                MyCombo.setText((models[index].decode("gb2312")))
                MyCombo.setMouseTracking(True)
                MyCombo.clicked.connect(self.modelclicked2)
                MyCombo.resize(MyCombo.sizeHint())
                MyCombo.setContextMenuPolicy(Qt.CustomContextMenu) 
                QtCore.QObject.connect(MyCombo, QtCore.SIGNAL('customContextMenuRequested (const QPoint&)'), self.modelbutton2menu)
                self.modellist2.setCellWidget(index,0,MyCombo)
                self.modellist2.resizeColumnsToContents()
        

    def groupBox2menu(self,point):
        if self.sender().objectName() == "groupBox2":
             self.tmpgroup = self.preanalysisplot
             self.imageindex = self.tabWidget1.currentIndex()-3
        else:
             self.tmpgroup = self.tmpimage
             self.imageindex = self.tabWidget.currentIndex()
             
        popMenu = QtGui.QMenu()
        tmp = QtGui.QAction(u'导入图片',self)
        self.connect(tmp,QtCore.SIGNAL('triggered()'),self.group2image)
        popMenu.addAction(tmp)
        popMenu.exec_(QtGui.QCursor.pos())

    def group2image(self):
        tmpDir = QtGui.QFileDialog.getOpenFileName() 
        if(len(tmpDir) > 0):
             path = unicode(tmpDir)
             image = QImage()
             image.load(path)
             martix = QMatrix()  
             martix.scale(float(self.tmpgroup.geometry().width())/image.width(),float(self.tmpgroup.geometry().height())/image.height())
             image=image.transformed(martix)
             self.tmpgroup.setPixmap(QPixmap.fromImage(image))
             self.tmpgroup.resize(image.width(),image.height())
             self.groupimagexdict[str(self.imageindex)] = path
        

    def nhdtablemenu(self, point):
        if self.sender().objectName() == "nhdtable":
             self.tmplistview = self.nhdtable
        else:
             self.tmplistview = self.tableView_2
             
        popMenu = QtGui.QMenu()
        tmp = QtGui.QAction(u'从剪切板导入数据',self)
        self.connect(tmp,QtCore.SIGNAL('triggered()'),self.loaddf2table)
        popMenu.addAction(tmp)
        tmp = QtGui.QAction(u'从文件导入数据',self)
        self.connect(tmp,QtCore.SIGNAL('triggered()'),self.loaddf2table)
        popMenu.addAction(tmp)
        popMenu.exec_(QtGui.QCursor.pos())

    def loaddf2table(self):
        if unicode(self.sender().text()) == u'从剪切板导入数据':
              df = pd.read_clipboard(header=None)
        else:
             tmpDir = QtGui.QFileDialog.getOpenFileName() 
             if(len(tmpDir) > 0):
                  path = unicode(tmpDir)
                  if ".csv" in path:
                       df = pd.read_csv(path,header=None)
                  elif ".xls" in path:
                       df = pd.read_excel(path,header=None)
                  else:
                       df = []
        df  =df.fillna("")
        df = df.astype(str)
        self.tmplistview.setModel(MyModel(df.values))
        self.tmplistview.resizeColumnsToContents()
        
    def uptoac3(self):
        self.tabs.setCurrentIndex(0)
        self.changelistmodel()

    def uptoac4(self):
        self.tabs.setCurrentIndex(2)

    def goac3(self):
        self.tabs.setCurrentIndex(3)
        self.changetabWidget()
        self.imageindex = self.tabWidget.currentIndex()
        self.input_image(self.tmpimage) 
         

    def initnhdmodel(self):
        self.groupimagexdict = {'-1':"",'0':'','1':'','2':'','3':'','-2':"",'-3':""}
        self.tableView_2.setModel(MyModel([]))          
        self.nhdtable.setModel(MyModel([]))
        self.datarange.setText('')
        self.datarange2.setText('')
        self.preanalysis.setText('')
        self.groupBrowser4.setText('')
        image = QImage()
        image.load('')
        martix = QMatrix()
        self.preanalysisplot.setPixmap(QPixmap.fromImage(image))
        self.changetabWidget()
   

    def changelistmodel(self):
        self.functionlist.clear()
        self.listmodels.clear()
        self.modelmask.setText(u'')
        self.functionmask.setText(u'')
        self.functionlist.addItems([i[0] for i in self.dblistsdict[self.dblists.currentIndex()]])
        self.functionlist.setCurrentRow(0)
        dirname = "model/%d"%self.dblists.currentIndex()
        if os.path.exists(dirname):
            self.listmodels.clear() 
            self.listmodels.addItems([i.decode("gb2312") for i in os.listdir(dirname)])
        self.listmodels.setCurrentRow(0)
        self.changemodel()
        self.changefunc()
        
    def changemodel(self):
        dirname = "model/%d"%self.dblists.currentIndex()
        try:
            modelname = unicode(self.listmodels.currentItem().text())
            path =os.path.join(dirname,modelname)
            if os.path.exists(path):
                self.modelmask.setText(json.loads(open(path,'r').read())['remarks'])
            else:
                self.modelmask.setText(u'')
        except:
            self.modelmask.setText(u'')        

    def changefunc(self):
        try:
            tdict = dict(self.cf.items(str(self.dblists.currentIndex())))
            funcname = unicode(self.functionlist.currentItem().text()).lower()
            if funcname in tdict.keys():
                self.functionmask.setText(u'')
                tmp = tdict[funcname].decode("gb2312").split('/n')
                for i in tmp:
                    self.functionmask.append(i)
            else:
                self.functionmask.setText(u'')
        except:
            self.functionmask.setText(u'')
    
    def enableops(self):
        for i in [self.xlabel,self.ylabel,self.figname,self.title,self.target,self.figuretype]:
            i.setEnabled(self.plotbz.isChecked())
        
    def exportdb(self):
        button=QMessageBox.question( self, u"提示框", u"是否确认导出数据？",QMessageBox.Ok|QMessageBox.Cancel, QMessageBox.Ok)
        bz = (button==QMessageBox.Ok)
        try:
            if bz:
                if not os.path.exists("result"):
                    os.makedirs("result")
                if self.sender().objectName() == "exportdata":
                     tablename =  self.dblistsinit[self.dblists.currentIndex()]["tablename"]
                     tableschma = self.dblistsinit[self.dblists.currentIndex()]["tableschma"]
                     filename,ok = QInputDialog.getText(self, u"保存文件名称",u"导出数据表文件名称:",QLineEdit.Normal, u'%s.xlsx'%tablename)
                     if ok :
                          if (".xlsx" not in unicode(filename)) or (".xls" not in unicode(filename)):
                               filename = unicode(filename)+".xlsx"
                          
                else:
                     tablename = "init_data"
                     tableschma = "initdb_sechma"
                     filename,ok = QInputDialog.getText(self, u"保存文件名称",u"导出原始数据表文件名称:",QLineEdit.Normal, 'init_data.xlsx')
                     if ok :
                          if (".xslx" not in filename) or (".xls" not in filename):
                               filename = filename+".xlsx"

                filename1,ok = QInputDialog.getText(self, u"保存文件名称",u"机组台月数文件名称:",QLineEdit.Normal, u'turnum.xlsx')
                if ok :
                     if (".xlsx" not in unicode(filename1)) or (".xls" not in unicode(filename1)):
                          filename1 = unicode(filename1)+".xlsx"
                     
                self.update_text(u'当前选择数据库为%s'%tablename)
                self.thread1 = Exmain(self)
                self.thread1.trigger.connect(self.update_text)
                self.thread1.setup(self.initdict,self.fiterdict,tablename,tableschma,filename,filename1)
                self.update_text(u'启动导出原始数据进程')
                self.thread1.start()
        except Exception,e:
            print unicode(e)
            self.update_text(u'请先加载数据库')        
        ## 导出原始数据，按照首页中筛选条件，送至工作进程中导出，按照refschema和筛选条件重合的条件进行筛选，然后导出

    def Getops(self):
        ops = {}
        ops['plotbz'] = self.plotbz.isChecked()
        ops['figname'] =unicode(self.figname.text())
        x = dict(zip(self.schma['chinese'],self.schma['head']))
        x[u'重复故障时间段'] = u'重复故障时间段'
        x[u'重复故障次数'] = u'重复故障次数'
        ops['axis'] =[unicode(i.data().toString()) for i in self.axiss.selectedIndexes()]
        ops['ylabel']= unicode(self.ylabel.text()).split(',')
        ops['xlabel']= unicode(self.xlabel.text())
        try:
            ops['yagg'] = x[unicode(self.yagg.currentText())]
        except:
            ops['yagg'] = ''
        try:
            ops['xagg'] = x[unicode(self.xagg.currentText())]
        except:
            ops['xagg'] =''
        ops['lines'] = [i[1] for i in self.yagglist]
        if len(ops['lines']) == 0:
            ops['lines'] = [[]]
        ops['legend'] = [i[0] for i in self.yagglist]
        ops['converge'] = [i[1] for i in self.xagglist]
        if len(ops['converge']) == 0:
            ops['converge'] = [[]]
        ops['xlegend'] = [i[0] for i in self.xagglist]
        ops['percent'] = self.percent.isChecked()
        ops['demoncheck'] = self.demoncheck.isChecked()
        ops['title'] = unicode(self.title.text())
        ops['topS'] = int(self.topS.value())
        ops['topN'] = int(self.topN.value())
        ops['PSIL_value'] = int(self.PSIL_value.value())
        ops['turnum'] = self.turnum
        ops['figname'] = 'image/'+ops['figname']+'.png'
        ops['xcheckBox'] = self.xcheckBox.isChecked()
        ops['ycheckBox'] = self.ycheckBox.isChecked()
        if ops['PSIL_value']<0:
            ops['PSIL_value'] = None
        if unicode(self.target.text())=="":
            ops['target'] = []    
        else:
            ops['target'] =[float(i) for i in unicode(self.target.text()).split(',')]
        ops['figuretype'] = unicode(self.figuretype.currentText())
        ops['repeattime'] = unicode(self.repeattime.currentText())
        return ops
        

    def mainansys(self):
        ops = self.Getops()       
        if len(self.axiss.selectedIndexes())>2 or len(self.axiss.selectedIndexes())==0:
            self.update_text(u"指标选择不能为空或者超过2个")
        elif ops['plotbz'] and ops['figname'] =='':
            QMessageBox.warning( self, u"提示框", u"导出图片名称不能为空", QtGui.QMessageBox.Yes)
        else:
            self.ops= ops
            self.update_text(u'已经启动数据分析，请稍后')
            self.rtime = time.time()
            self.redf=[]
            self.thread = Pmain(self)
            self.thread.trigger.connect(self.savedf)
            self.thread.setup(self.ansys,self.jdf,ops)  
            self.thread.start()

    def savedf(self,re):
        self.redfs =re
        self.update_text(u'数据分析完成,用时%0.2fs'%(time.time()-self.rtime))
        ops =self.ops
        if ops['plotbz']:
            if ops['percent']:
                fig = pl.figure(figsize = (12.57,6.16), dpi =150)
                re['indf'].plot(kind = ops['figuretype'])
                pl.xlabel(ops['xlabel'])
                pl.ylabel(ops['ylabel'])
            else:
                redf1= re['redf1']
                redf2= re['redf2']
                if self.dblists.currentIndex() ==2:
                ## 针对重复故障级数据特定的画图规则                   
                    fig = pl.figure(figsize = (9,9), dpi =120)
                    if  ops['figuretype']== 'pie':
                        redf1.plot(kind = 'pie',autopct='%.2f',fontsize=13,colormap='Reds')
                    else:
                        if ops['target']!= []:
                            redf1[u'目标值'] = ops['target']
                        redf1.plot(kind = ops['figuretype'])
                        legend = redf1.columns
                        pl.xlabel(ops['xlabel'])
                        pl.ylabel(ops['ylabel'][0])                          
                        pl.legend(legend,loc = 'best',fontsize = 'small')
                        
                else:
                    if len(ops['axis']) ==1:
                        if ops['legend'] ==[]:
                            ops['legend'] = redf1.index
                        if  ops['figuretype']== 'pie':
                            legend = redf1.index
                            fig = pl.figure(figsize = (9,9), dpi =120)
                            redf1.columns = [ops['axis'][0]]
                            redf1[ops['axis'][0]].plot(kind = 'pie',autopct='%.2f',fontsize=13,colormap='Reds')
                            pl.legend(legend,loc = 'lower right',fontsize = 'small')
                        else:
                            fig = pl.figure(figsize = (12.57,6.16), dpi =150)
                            redf1.plot(kind = ops['figuretype'])
                            pl.xlabel(ops['xlabel'])
                            pl.ylabel(ops['ylabel'][0])
                            if ops['target']!= []:
                                pl.plot(ops['target'])
                                ops['legend'].insert(0,u'目标值')   
                            pl.legend(ops['legend'],loc = 'best',fontsize = 'small')
                    if len(ops['axis']) ==2:
                        fig = pl.figure(figsize = (12.57,6.16), dpi =150)
                        ax1=fig.add_subplot(111)
                        ax2=ax1.twinx()
                        if ops['legend'] ==[]:
                            legend1 = redf1.index
                        else:
                            legend1 = ops['legend']
                        ax1.plot(redf1,linestyle = '-',linewidth = 2,color = 'red')
                        ax1.set_xticks(range(len(redf1)))
                        ax1.set_xticklabels(redf1.index)
                        ax1.set_xlabel(ops['xlabel'], fontsize=12)
                        ax1.set_ylabel(ops['ylabel'][0],fontsize=12)
                        ax1.legend(legend1,loc='upper left',fontsize=10,shadow=True)
                        ax2.plot(redf2,linestyle = '-',linewidth = 2,color = 'blue')
                        if len(ops['ylabel']) ==2:
                            ax2.set_ylabel(ops['ylabel'][1],fontsize=12)
                        ax2.legend(legend1,loc='upper right',fontsize=10,shadow=True)
            pl.tight_layout()
            pl.title(ops['title'],fontsize=12)
            pl.savefig(ops['figname'])
            pl.close('all')
            self.update_text(u'图片保存完成')

    def savefiles(self):
        try:
            if len(self.redfs) == 0:
                QMessageBox.warning( self, u"提示框", u"目前没有结果文件需要保存,请先进行数据分析",QMessageBox.Yes)
            else:
                if self.ansys ==tur_ansys:
                    defults = ['Aggregated_results.xlsx','Index_calculation_result.xlsx']
                    svfiles = ['redf','indf']
                elif self.ansys ==fau_ansys:
                    defults = ['Aggregated_fault_results.xlsx','Fault_calculation_result.xlsx']
                    svfiles = ['redf2','redf1']
                elif self.ansys ==ref_ansys:
                    defults = ['Refault_results.xlsx','Refault_calculation_result.xlsx']
                    svfiles = ['redf','redf1']
                else:
                    pass
                x = dict(zip(self.schma['head'],self.schma['chinese']))
                self.update_text(u'共计%d个文件需要保存，请输入保存文件名称'%(len(defults)))                    
                for i,j in enumerate(svfiles):
                    filename,ok = QInputDialog.getText(self, u"保存文件名称",u"文件名称:",QLineEdit.Normal, defults[i])
                    if ok :
                        self.update_text(u'正在保存第%d个文件'%(i+1))
                        try:
                            if '.xls' not in unicode(filename):
                                filename = unicode(filename)+u'.xlsx'
                            else:
                                filename = unicode(filename)
                        except :
                            self.update_text(u'文件名称有误，以默认名称%s存储'%defults[i])
                            filename = defults[i]
                        newcols = []
                        for colindex in self.redfs[j].columns:
                            if colindex in x.keys():
                                newcols.append(x[colindex])
                            else:
                                newcols.append(colindex)
                        newindexs = []
                        for index in self.redfs[j].index.names:
                            if index in x.keys():
                                newindexs.append(x[index])
                            else:
                                newindexs.append(index)        
                        self.redfs[j].columns = newcols
                        self.redfs[j].index.names = newindexs
                        self.redfs[j].to_excel(u'result/'+filename,index =True)
                        self.update_text(u'文件%s结果保存成功'%(filename))
                self.update_text(u'所有文件保存成功')
        except Exception,e:
            self.update_text(unicode(e))

    def initagg(self):
        if self.sender().objectName() == 'xagg':
            self.converge.setEnabled(False)
            self.converge.setModel(MyModel([]))
            self.xcheckBox.setChecked(False)
        else:
            self.lines.setEnabled(False)
            self.lines.setModel(MyModel([]))
            self.ycheckBox.setChecked(False)        

    def changeagg(self):
        if self.sender().objectName() == 'xcheckBox':
            self.xagglist=[]
            if self.sender().isChecked() :
                self.converge.setEnabled(True)
            else:
                self.converge.setEnabled(False)
                self.converge.setModel(MyModel([]))
        else:
            self.yagglist=[]
            if self.sender().isChecked() :
                self.lines.setEnabled(True)
            else:
                self.lines.setEnabled(False)
                self.lines.setModel(MyModel([]))

    def addnewagg(self):
        key = unicode(self.currentactive.currentText())
        ref= unicode(self.repeattime.currentText()) 
        x = dict(zip(self.schma['chinese'],self.schma['head']))
        self.putform1 = AddaggDlg()
        if self.dblists.currentIndex() ==2 and key ==u'重复故障时间段':
            if ref == u"":
                tmp1 = [name for name in self.jdf.columns if name.find("nfm")==0]
            else:
                tmp1 = [ref]
            self.putform1.setup(key,pd.Series(tmp1).astype(str))
        elif self.dblists.currentIndex() ==2 and key ==u'重复故障次数':
            if ref==u"":
                self.update_text(u'请先确认重复故障时间段')
            else:
                self.putform1.setup(key,self.jdf[ref].drop_duplicates())
        else:
            name = x[key]
            self.putform1.setup(key,self.jdf[name].drop_duplicates())
        self.putform1.button.clicked.connect(self.GetOp1)
        
    def deleteagg(self):
        if self.currentactive.objectName()=='yagg':
            tt = self.yagglist
            tt1 = self.lines
        else:
            tt = self.xagglist
            tt1 = self.converge
        model= tt1.model()
        for index in tt1.selectedIndexes():
            key = unicode(model.data(model.index(index.row(),0)).toString())
            tt.pop(zip(*tt)[0].index(key))
        tt1.setModel(MyModel([[i[0],str(i[1])] for i in tt],[u'区间名称',u'区间列表']))

    def uptoac2(self):
        self.tabs.setCurrentIndex(0)
        self.changelistmodel()

    def filtercols(self):
        try:
            self.putform = FilterDlg()
            self.putform.setup(self.df,self.schma,self.fiterdict,self.dblists.currentIndex())
            self.putform.button.clicked.connect(self.GetOp)
        except Exception ,e:
            print e
            self.update_text(u'请先加载数据库')

    def GetOp1(self):
        key =unicode(self.putform1.aggname.text())
        if key==u'':
            self.update_text(u"区间名称不能为空")
        else:
            self.putform1.close()
            if self.currentactive.objectName()=='yagg':
                tt = self.yagglist
                tt1 = self.lines
            else:
                tt = self.xagglist
                tt1 = self.converge
            if len(tt) ==0:
                tt.append([key,self.putform1.dictx[key]])
            elif key in zip(*tt)[0]:
                index = zip(*tt)[0].index(key)
                t = tt[index][1]
                t.extend(self.putform1.dictx[key])
                tt[index][1] = t
            else:
                tt.append([key,self.putform1.dictx[key]])
            tt1.setModel(MyModel([[i[0],str(i[1])] for i in tt],[u'区间名称',u'区间列表']))
        
    def GetOp(self):
        self.putform.close()
        self.fiterdict = self.putform.dictx
        x = dict(zip(self.schma['head'],self.schma['chinese']))
        ## 针对重复故障数据库
        x[u'重复故障时间段'] = u'重复故障时间段'
        x[u'重复故障次数'] = u'重复故障次数'
        ## 针对重复故障数据库
        self.fiterlist.setModel(MyModel([[x[i],len(self.fiterdict[i])] for i in self.fiterdict.keys()],[u'字段名称',u'已选']))
        self.fiterlist.resizeColumnsToContents()

    def onButtonPopMenu(self, point):
        t = self.sender().objectName()
        if t == 'fiterlist':
            self.popMenu.exec_(self.fiterlist.mapToGlobal(point))
        elif t == 'converge':
            self.currentactive = self.xagg
            self.popMenu1.exec_(self.converge.mapToGlobal(point))
        else:
            self.currentactive = self.yagg
            self.popMenu1.exec_(self.lines.mapToGlobal(point))

    def deletkey(self):
        x = dict(zip(self.schma['chinese'],self.schma['head']))
        model= self.fiterlist.model()
        for index in self.fiterlist.selectedIndexes():
            key = unicode(model.data(model.index(index.row(),0)).toString())
            self.fiterdict.pop(x[key])
            self.GetOp()
        

    def init(self):
        if os.path.exists('serverinit.tmp'):
            with open('serverinit.tmp','r') as f:
                init = json.loads(f.read())
                self.serverhost.setText(init['serverhost'])
                self.username.setText(init['username'])
                self.password.setText(init['password'])
                self.servername.setText(init['servername'])

    def selecttable(self):
        key = self.dblists.currentIndex()
        if key in self.dblistsinit.keys():
             xdict =  self.dblistsinit[key]
             tablename =xdict["tablename"]
             tableschma = xdict["tableschma"]
             self.ansys =xdict["ansys"]
             self.percent.setChecked(xdict["percentChecked"])
             self.percent.setEnabled(xdict["percentEnabled"])
             self.PSIL_value.setEnabled(xdict["PSIL_value"])
             self.topS.setEnabled(xdict["topS"])
             self.topN.setEnabled(xdict["topN"])
             self.demoncheck.setChecked(xdict["demoncheckChecked"])
             self.demoncheck.setEnabled(xdict["demoncheckEnabled"])
             self.repeattime.setEnabled(xdict["repeattime"])
             self.axiss.setModel(MyModel(self.dblistsdict[self.dblists.currentIndex()],[u'指标名称']))
             self.axiss.resizeColumnsToContents()
             self.aggdata  = self.dblistsdict[self.dblists.currentIndex()]
             try:
                 if self.conn != None:
                     self.conn.close()
                 init = {'servername':unicode(self.servername.text()),'username':unicode(self.username.text()), 'password':unicode(self.password.text()), 'serverhost':unicode(self.serverhost.text())}
                 self.initdict = init
                 x = Mysql(dbname =unicode(self.servername.text()),sqltype = 'postgre',user=unicode(self.username.text()), password=unicode(self.password.text()), host=unicode(self.serverhost.text()))
                 self.engine = create_engine("postgresql://%s:%s@%s/%s"%(init['username'],init['password'],init['serverhost'],init['servername']),encoding='utf-8')
                 self.df = pd.read_sql_query('select * from %s'%tablename,con=self.engine)
                 self.schma = pd.read_sql_query('select * from %s'%tableschma,con=self.engine)
                 self.turnum = pd.read_sql_query('select * from turnum',con=self.engine)
                 self.conn = x.conn
                 self.update_text(u'加载数据表完成,共计%d行' % (len(self.df)))
                 with open('serverinit.tmp','w') as f:
                     f.write(json.dumps(init))
                 self.jdf = self.df
                 self.xagglist=[]
                 self.yagglist=[]
                 self.fiterdict ={}
                 self.axiss.clearSelection()
                 self.axiss.setCurrentIndex(QModelIndex(self.axiss.model().index(0,0)))
                 self.fiterlist.setModel(MyModel([]))
                 self.refreshmodellist()
             except Exception,e:                 
                 self.update_text(unicode(e))
        else:
             print 1

    def update_text(self,msg):
        self.message.append(msg)

    def filterdf(self):
        try:
            key = self.dblists.currentIndex()
            if key in self.dblistsinit.keys():
                 if len(self.fiterdict.keys()) == 0:
                     self.jdf = self.df
                 else:
                     self.jdf = self.df
                     for i in self.fiterdict:
                         ## 针对重复故障数据库
                         if i == u'重复故障时间段' and  self.dblists.currentIndex() ==2:
                             tmp1 = [name for name in self.jdf.columns if name.find("nf")!=0]
                             tmp1.extend(self.fiterdict[i])
                             self.jdf = self.jdf[tmp1]
                         elif i == u'重复故障次数' and self.dblists.currentIndex() ==2 :
                             tmp1 = [name for name in self.jdf.columns if name.find("nfm")==0]
                             commend = "|".join(["(self.jdf['%s'].isin(self.fiterdict[i]))"%j for j in tmp1])
                             bz = eval(commend)
                             self.jdf = self.jdf[bz]
                         ## 针对重复故障数据库
                         else:
                             self.jdf=self.jdf[self.jdf[i].isin(self.fiterdict[i])]
                 self.update_text(u'筛选数据完成,共计%d行' % (len(self.jdf)))
                 x = dict(zip(self.schma['head'],self.schma['chinese']))
                 self.xagg.clear()
                 self.yagg.clear()
                 if self.dblists.currentIndex() ==2:
                 ## 针对重复故障数据库
                     t = [x[i] for i in self.jdf.columns if i.find("nf")!=0]
                     t.extend([u'重复故障时间段',u'重复故障次数'])
                 else:
                     t = [x[i] for i in self.jdf.columns]
                 ## 针对重复故障数据库         
                 t.insert(0,'')            
                 self.xagg.addItems(t)
                 self.yagg.addItems(t)
                 tmp1 = [name for name in self.jdf.columns if name.find("nfm")==0]
                 tmp1.sort()
                 tmp1.insert(0,'')    
                 self.repeattime.clear()
                 self.repeattime.addItems(tmp1)
                 self.tabs.setCurrentIndex(1)
            else:
                 self.tabs.setCurrentIndex(2)
                 self.refreshmodellist2()
                 self.initnhdmodel()

        except Exception,e:
            print unicode(e)
            self.update_text(u'请先加载数据库')


    def saveparameters(self):
        ops= self.Getops()
        ops.pop("turnum")
        ops['dbtype'] = self.dblists.currentIndex()
        self.putform2 = Userdiaglog()
        self.putform2.setup(ops)
        self.putform2.button.clicked.connect(self.GetOp2)

    def GetOp2(self):
        ops= self.putform2.ops
        ops['modelname'] = unicode(self.putform2.name.text())
        ops['remarks'] = unicode(self.putform2.remarks.toPlainText())
        dirname = "model/%d"%self.putform2.ops["dbtype"]
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        try:
            shutil.copy(ops['figname'],ops['figname'].replace("image/","icon/"))
        except:
            pass
        ops['figname'] = ops['figname'].replace("image/","icon/")
        bz = True
        if os.path.exists(dirname+"/"+ops['modelname']):
            button=QMessageBox.question( self, u"提示框", u"模块名称已存在，是否确认覆盖？",QMessageBox.Ok|QMessageBox.Cancel, QMessageBox.Ok)
            bz = (button==QMessageBox.Ok)
        if bz:        
            with open(dirname+"/"+ops['modelname'],"w") as f:
                f.write(json.dumps(self.putform2.ops))
            self.putform2.close()
            self.update_text(u"功能模块：%s 已保存"%ops['modelname'])
            self.refreshmodellist()

    def refreshmodellist(self):
        # 刷新modellist列表
        self.modeldict ={}
        self.modellist.clear()
        dirname = "model/%d"%self.dblists.currentIndex()
        if os.path.exists(dirname):            
            models = os.listdir(dirname)
            for index in range(len(models)):
                self.modeldict[models[index].decode("gb2312")] = json.loads(open(os.sep.join([dirname,models[index]]),"r").read())
                MyCombo = QtGui.QPushButton(models[index].decode("gb2312"),self)
                MyCombo.setText((models[index].decode("gb2312")))
                MyCombo.setMouseTracking(True)
                MyCombo.clicked.connect(self.modelclicked)
                MyCombo.resize(MyCombo.sizeHint())
                self.modellist.setCellWidget(index,0,MyCombo)
                self.modellist.resizeColumnsToContents()
                 



    def modelclicked(self):
        key =unicode(self.sender().text())
        self.putform3 = Userselect()
        self.putform3.setup(self.modeldict[key]['figname'],self.modeldict[key]['remarks'])
        self.currentkey = key
        self.putform3.button.clicked.connect(self.GetOp3)
        

    def Ops_analysis(self,ops):
        self.figuretype.setCurrentIndex(self.figuretype.findText(ops['figuretype']))
        x = dict(zip(self.schma['head'],self.schma['chinese']))
        x[u'重复故障时间段'] = u'重复故障时间段'
        x[u'重复故障次数'] = u'重复故障次数'
        if ops['xagg'] != u"":
            self.xagg.setCurrentIndex(self.xagg.findText(x[ops['xagg']]))
        else:
            self.xagg.setCurrentIndex(0)
        if ops['yagg'] != u"":
            self.yagg.setCurrentIndex(self.yagg.findText(x[ops['yagg']]))
        else:
            self.yagg.setCurrentIndex(0)
        self.repeattime.setCurrentIndex(self.repeattime.findText(ops['repeattime']))
        self.plotbz.setChecked(ops['plotbz'])
        self.enableops()
        self.percent.setChecked(ops['percent'])
        self.demoncheck.setChecked(ops['demoncheck'])
        self.xcheckBox.setChecked(ops['xcheckBox'])
        self.ycheckBox.setChecked(ops['ycheckBox'])
        self.xlabel.setText(ops['xlabel'])
        if isinstance(ops['ylabel'],list):
            self.ylabel.setText(",".join(ops['ylabel']))
        else:
            self.ylabel.setText(ops['ylabel'])
        self.title.setText(ops['title'])
        name =ops['figname'].replace("image/","")
        name = ".".join(name.split(".")[:-1]).split("/")[-1]
        self.figname.setText(name)
        self.target.setText(",".join([unicode(i) for i in ops['target']]))
        try:
            self.PSIL_value.setValue(ops["PSIL_value"])
        except:
            self.PSIL_value.setValue(-1)
        self.topS.setValue(ops["topS"])
        self.topN.setValue(ops["topN"])
        self.xagglist = [list(i) for i in zip(ops['xlegend'],ops['converge'])]
        self.yagglist = [list(i) for i in zip(ops['legend'],ops['lines'])]
        tmp = [",".join([unicode(j) for j in i]) for i in ops['converge']]
        t = [list(i) for i in zip(ops['xlegend'],tmp)]
        self.converge.setModel(MyModel(t,[u'区间名称',u'区间列表']))
        tmp1 = [",".join([unicode(j) for j in i]) for i in ops['lines']]
        t1 = [list(i) for i in zip(ops['legend'],tmp1)]
        self.lines.setModel(MyModel(t1,[u'区间名称',u'区间列表']))
        self.axiss.clearSelection()
        for i in ops['axis']:
            self.axiss.setCurrentIndex(QModelIndex(self.axiss.model().index(self.aggdata.index([i]),0)))
        
               
    def GetOp3(self):
        #获取被点击的模型型号信号源的key
        self.putform3.close()
        self.Ops_analysis(self.modeldict[self.currentkey])

    def modifymodel(self):
        dirname = "model/%d"%self.dblists.currentIndex()
        if os.path.exists(dirname) and len(os.listdir(dirname))!=0:
            self.putform4 = Modify()
            self.putform4.setup(dirname)
            self.putform4.button.clicked.connect(self.refreshmodellist)
            self.putform4.button1.clicked.connect(self.refreshmodellist)
            self.putform4.qbutton.clicked.connect(self.refreshmodellist)
            

if __name__ == "__main__" :
    if not os.path.exists('image'):
        os.makedirs('image')
    if not os.path.exists('result'):
        os.makedirs('result')
    app = QtGui.QApplication(sys.argv)
    window = MyApp()
    window.show()
    sys.exit(app.exec_())

