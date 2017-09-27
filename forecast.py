#!/usr/bin/python
# -*- coding: UTF-8 -*-

from PyQt4 import QtCore, QtGui, uic
from PyQt4.QtGui import *
from PyQt4.QtCore import *
import FileDialog,sys,os
import pandas as  pd
import ConfigParser,copy,markov
import datetime

if getattr(sys, 'frozen', None):
     basedir = sys._MEIPASS
else:
     basedir = os.path.dirname(__file__)
     
qtCreatorFile = os.path.join(basedir, "forecast.ui" )
Ui_MainWindow, QtBaseClass = uic.loadUiType(qtCreatorFile)

class Exmain(QtCore.QThread):
    trigger = QtCore.pyqtSignal(dict)
  
    def __init__(self, parent=None):
        super(Exmain, self).__init__(parent)

    def setup(self,data,funcs,ps):
        self.funcs = funcs
        self.data = data
        self.ps = ps
    def run(self):
        results = {}
        for func in self.funcs:
             results[func] = markov.do_main(self.data,self.ps)
        self.trigger.emit (results)


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

def dictxy(dictx):
    for key in dictx.keys():
         dictx[key] = dictx[key].decode("gb2312")
    return dictx

class FullScreen(QDialog):
    def __init__(self,parent=None):
        super(FullScreen,self).__init__(parent)
        
    def setup(self,result,dict1):
        self.setWindowTitle(u'模型对比')
        df = pd.DataFrame()
        for name in result:
             tmp = result[name]['result']
             columns = tmp.columns.tolist()
             tmp1 = pd.Series(['']*len(tmp.columns),index = columns)
             tmp1.iloc[0] = dict1[name]+u'模型结果：'
             df = df.append(tmp1.to_frame().T)
             df = df.append(tmp)
             df = df.append(pd.Series(['']*len(tmp.columns),index = columns).to_frame().T)
        table = QTableView()
        table.verticalHeader().setVisible(False)
        table.setModel(MyModel(df.fillna("").values,columns)) 
        layout = QVBoxLayout()
        layout.addWidget(table)
        self.setLayout(layout)
        self.showFullScreen()
        screen = QtGui.QDesktopWidget().screenGeometry()
        self.resize(screen.width()*0.9, screen.height()*0.9)
        size = self.geometry()
        self.move((screen.width()-size.width())/2,(screen.height()-size.height())/2-40)
        self.show()
        

class MyApp(QtGui.QMainWindow, Ui_MainWindow):
    def __init__( self ):
        QtGui.QMainWindow.__init__( self )
        Ui_MainWindow.__init__( self )      
        self .setupUi( self )
        self.init_data.verticalHeader().setVisible(False)
        screen = QtGui.QDesktopWidget().screenGeometry()
        size = self.geometry()
        self.imagetab =QTabWidget()
        layout = QVBoxLayout()
        layout.addWidget(self.imagetab)
        self.images.setLayout(layout)
        self.move((screen.width()-size.width())/2,(screen.height()-size.height())/2-40)
        self.connect(self.loaddata1,QtCore.SIGNAL('clicked()'), self.loaddata)
        self.connect(self.loaddata2,QtCore.SIGNAL('clicked()'), self.loadfile)
        self.connect(self.modelfuncs,QtCore.SIGNAL('itemClicked (QTableWidgetItem *) '), self.changep)
        self.connect(self.button1,QtCore.SIGNAL('clicked()'), self.predict)
        self.connect(self.button2,QtCore.SIGNAL('clicked()'), self.fullview)
        self.connect(self.button3,QtCore.SIGNAL('clicked()'), self.savedata)
        self.connect(self.savep,QtCore.SIGNAL('clicked()'), self.saveps)
        self.results = {}
        cf = ConfigParser.ConfigParser()
        cf.read('icon/init.ini')
        dict1 = dictxy(dict(cf.items('forecast')))
        self.dict1 = dict1
        self.modelfuncs.setRowCount(len(dict1))
        self.modelfuncs.setColumnCount(2)
        self.modelfuncs.setHorizontalHeaderLabels([u'选中',u'模型名称'])
        self.functions = {}
        self.functions_index = []
        for i,key in enumerate(dict1.keys()):
           combo = QCheckBox()
           self.modelfuncs.setCellWidget(i, 0, combo)
           self.modelfuncs.setItem(i, 1, QTableWidgetItem(dict1[key]))
           self.functions[key] = combo
           self.functions_index.append(key)
        self.modelfuncs.resizeColumnsToContents() 
        self.modelfuncs.resizeRowsToContents()
        ## QToolbox
        dict2 = dictxy(dict(cf.items('introduce')))
        self.introduction =QToolBox()
        for name in dict2.keys():
             introduction = QTextEdit()
             introduction.setReadOnly(True)
             introduction.append(dict2[name])
             groupbox=QGroupBox()
             vlayout=QVBoxLayout(groupbox)
             vlayout.setMargin(10)
             vlayout.setAlignment(Qt.AlignCenter)
             vlayout.addWidget(introduction)
             vlayout.addStretch()  
             self.introduction.addItem(groupbox,dict1[name])
        layout = QVBoxLayout()
        layout.addWidget(self.introduction)
        self.ingroup.setLayout(layout)
        ## QTabWidget
        dict3 = dictxy(dict(cf.items('parameter')))
        self.parametertab =QTabWidget()
        self.parmet_Edits ={}
        for name in dict3.keys():
             groupbox=QGroupBox()
             vlayout=QGridLayout(groupbox)
             tempd = {}
             for i,key in enumerate(dict3[name].split(",")):
                  if key!="":
                       tmps = key.split("&")
                       keyname = tmps[0]
                       if len(tmps)==2:
                            keyvalue = tmps[1]
                       else:
                            keyvalue = u''
                       temp = QLineEdit()
                       temp.setText(keyvalue)
                       vlayout.addWidget(QLabel(keyname),i,0)
                       vlayout.addWidget(temp,i,1)
                       tempd[keyname] = temp
             self.parmet_Edits[name] = tempd
             self.parametertab.addTab(groupbox,dict1[name])
        layout = QVBoxLayout()
        layout.addWidget(self.parametertab)
        self.parameters.setLayout(layout)
        self.maindf = pd.DataFrame()

    def savedata(self):
        N= self.modelfuncs.currentRow()
        if N==-1:
             self.update_text(u'请先选择模型')
        else:             
             model = self.functions_index[N]
             df = self.results[model]['result']
             name,ok = QInputDialog.getText(self, u"输入结果保存文件名称",u"文件名称:",QLineEdit.Normal, u"%s_result.xlsx"%self.dict1[self.functions_index[N]])
             if ok:
                  if ".xls" not in name:
                       name = name+".xlsx"
                  df.to_excel(unicode(name),index=False)
                  self.update_text(u'结果文件保存成功')
             

    def fullview(self):
        self.putform = FullScreen()
        self.putform.setup(self.results,self.dict1)
             

    def saveps(self):
        N= self.parametertab.currentIndex()
        tmp = self.parmet_Edits[self.functions_index[N]]
        temp ={}
        for key in tmp:
             temp[key] = unicode(tmp[key].text())
        name,ok = QInputDialog.getText(self, u"输入参数文件名称",u"参数文件名称:",QLineEdit.Normal, u"%s参数.ini"%self.dict1[self.functions_index[N]])
        if ok:
             if ".ini" not in name:
                  name = name+".ini"
             f = open(unicode(name).encode("gb2312"),"w")
             for key in temp:
                  f.write((u"%s = %s\n"%(key,temp[key])).encode("gb2312"))
             f.close()
             self.update_text(u'模型参数保存成功')

    def changep(self):
        N= self.sender().currentRow()
        self.parametertab.setCurrentIndex(N)
        self.introduction.setCurrentIndex(N)
        model = self.functions_index[N]
        if model in self.results.keys():
             self.result_table.setModel(MyModel(self.results[model]['result'].fillna("").values,self.results[model]['result'].columns.tolist())) 

    def setdate(self):
        cols = list(self.data.columns[1:])
        stime = datetime.datetime.strptime(cols[0],"%Y-%m").date()
        etime = datetime.datetime.strptime(cols[-1],"%Y-%m").date()
        if etime.month>=10:
             ptime = datetime.date(etime.year+1,etime.month-9,1)
        else:
             ptime = datetime.date(etime.year,etime.month+3,1)
        tmp = [stime,etime,ptime]
        for index,obj in enumerate([self.startdate,self.enddate,self.predictdate]):
             obj.setDate(tmp[index])

    def loaddata(self):
        if len(self.maindf)!=0:
             self.data = self.maindf
             self.setdate()
             self.init_data.setModel(MyModel(self.data.fillna("").values,self.data.columns.tolist()))

    def loadfile(self):
        tmpDir = QtGui.QFileDialog.getOpenFileName() 
        if(len(tmpDir) > 0):
             path = unicode(tmpDir)
             df =pd.read_excel(path)
             self.data = df
             self.setdate()
             self.init_data.setModel(MyModel(self.data.fillna("").values,self.data.columns.tolist())) 

    def predict(self):
        try:
             data = self.data
             funcs = [key  for key in self.functions if self.functions[key].isChecked()]   
             ps = {"pdate":self.predictdate.date().toPyDate(),"sdate":self.startdate.date().toPyDate(),"edate":self.enddate.date().toPyDate()}
             if len(funcs)!=0:
                  for func in funcs:
                       tmp = self.parmet_Edits[func]
                       temp ={}
                       for key in tmp:
                            temp[key] = unicode(tmp[key].text())
                       ps[func] = temp
                  self.update_text(u'启动数据预测')
                  self.thread1 = Exmain(self)
                  self.thread1.trigger.connect(self.getresult)
                  data = copy.deepcopy(self.data)
                  self.thread1.setup(data,funcs,ps)
                  self.thread1.start()
        except Exception ,e:
             self.update_text(unicode(e))
             self.update_text(u'请先导入数据')

    def input_image(self,tmp,path): 
        tmp.setPixmap(QPixmap(path))
        tmp.setScaledContents(True)
        tmp.resize(tmp.geometry().width(),tmp.geometry().height())

    def getresult(self,result):
        self.results = result
        self.update_text(u'数据预测完成')
        self.imagetab.clear()
        for model in self.results.keys():
             for index,path in enumerate(self.results[model]['figs']):
                  groupbox=QGroupBox()
                  temp = QLabel("")
                  self.input_image(temp,path)
                  vlayout=QVBoxLayout(groupbox)
                  vlayout.addWidget(temp)
                  self.imagetab.addTab(groupbox,u"%s.%d"%(model,index))
                  
    def update_text(self,msg):
        self.message.append(msg)


if __name__ == "__main__" :
    app = QtGui.QApplication(sys.argv)
    window = MyApp()
    window.show()
    sys.exit(app.exec_())
