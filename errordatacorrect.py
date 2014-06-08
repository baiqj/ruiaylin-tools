# author        : ruichao.lin(baiqiao)
# func          : add new table to DSS system 
# lang          : python 
# version       : python 2.4
# date          : 2012-10-10
# for           : GNU - linux 
# encoding      : UTF-8
#!/home/oracle/dbapython/bin/python
# -*- coding: utf-8 -*-
"""
 ScriptName: trade core system migration , exception data .py
 Author: ruiay(baiqiao)
 Create Date: 2013-08-24
 Function: move those data from the product system to archived system .
"""
import re
import sys,os
import math
import time
import commands,string
from signal import SIGTERM
import cx_Oracle
import MySQLdb
from optparse import OptionParser
import ConfigParser
from termcolor import colored
import logging ,log
import threading
import multiprocessing 
import Queue

mylock = thread.allocate_lock()  #Allocate a lock  
num=0  #Shared resource  

class __global__(object):
    """docstring for __global__"""
    taskqueue  = Queue.Queue(maxsize = 2000)
    errorQueue = Queue.Queue(maxsize= 1000)
    runningqueue={}
    thread_execinfo={}
    dc = {};
    #ds_statistics={}
    gathernext={}
    connectionpool={}
    dsconnpoolmax={}
    dslists=[]
    dshash={};
    buslists=[]
    bus_distinct={}
    bushash={}
    dbconf={}
    ThreadArray=[]
    opt = {}
    opt['-f'] = None
    running=True
    

gv = __globals__();
######################## tools func ######################### 
def changetomap(kvs):
    tmpkvs={}
    for x in kvs:
        tmpkvs[x[0]] = x[1].strip()
    return tmpkvs
######################## connection func ######################### 
def getOraConnTns(tns):
    conn = cx_Oracle.connect('devdba','devdba',tns,threaded = True)
    #connection = cx_Oracle.connect('/', mode=cx_Oracle.SYSDBA)
    return conn

def getOraConnTnsUP(user,passw,tns):
    conn = cx_Oracle.connect(user,passw,tns,threaded = True)
    #connection = cx_Oracle.connect('/', mode=cx_Oracle.SYSDBA)
    return conn
######################## fund_bill thread classs ######################### 
# fund_bill  
# set lines 1000 pagesize 1000
# select trade_no,bill_no,to_char(gmt_create,'yyyy-mm-dd hh24:mi:ss') from 
#       TRADECORE00.tmp_trd_fnd_bil_00_1302 a where trade_no not in (
#   select trade_no from TRADECORE00.tmp_unc_trade_1302_00 b)
#    nd gmt_create>to_date('20130618 16:16:33','yyyymmdd hh24:mi:ss');
#                       tmp_trd_fnd_bil_00_1302         tmp_unc_trade_1302_00    
# construct the infos for the query
def getEarliestTime(imount):
     conn = getOraConnTnsUP('migrate2','migrate2','tradehis_pri')
     sql = ''' select to_char(min(created),'yyyymmdd hh24:mi:ss') from dba_objects where lower(object_name) like :tmps and object_type='TABLE' '''
     cur = conn.cursor()
     args1 = {}
     args1['tmps'] = 'tmp_unc_trade_'+ imount[2:6] + '%'
     try :
         cur.execute(sql,args1)
         rs=cur.fetchall()
         dt = rs[0][0]
         return dt
     except Exception ,e:
         print e
     finally:
         cur.close()
         conn.close() 

def getfundbillmeta():
    conn = getOraConnTnsUP('stats_user','stats_user','tooldb_pri')
    cur = conn.cursor()
    sql = ''' select TABLE_OWNER_ONLINE , TABLE_NAME_ONLINE_PRE ,DB_LINK from trade_correct_errd_meta where TABLE_NAME_ONLINE_PRE like :tmp1 and TABLE_OWNER_ONLINE not like :tmp2 order by  1 '''
    args1 = {}
    args1['tmp1'] = 'trade_fund_bill' + '%'
    args1['tmp2'] = 'tradecore' + '%'
    dicts = {}
    try :
        cur.execute(sql,args1)
        rs=cur.fetchall()
        for x in rs : 
            dicts[x[1]] = x[0] +'-'+ x[2]
        return dicts
    except Exception ,e:  
        print e
    finally:
        cur.close()
        conn.close() 

def addTaskToQueue(meta,imonth,queue):
    #fb early time 
    earlytime=getEarliestTime(imonth) 
    fbs = meta.keys()
    fbs.sort()
    for fb in fbs : 
       tableowner,tnsname =  meta[fb].split('-')
       dblink = tnsname
       prodtable = fb
       position= fb[-3:]
       tmptable = 'tmp_trd_fnd_bil_'+position+'_'+imonth[2:6] 
       unctrtable='TMP_UNC_TRADE_'+imonth[2:6]+'_' + position
       t3= tableowner+'.'+ tmptable    
       t4= tableowner+'.'+ unctrtable 
       sqltextTemplate= '''
select trade_no,bill_no,to_char(gmt_create,'yyyy-mm-dd hh24:mi:ss') from %s a where trade_no not in ( select trade_no from %s b) and gmt_create>to_date('%s','yyyymmdd hh24:mi:ss')
 '''
       sqltext  = sqltextTemplate %(t3,t4,earlytime)
       task = TASKINFO(tnsname,dblink,tmptable,prodtable,tableowner,imonth,position ,sqltext)
       queue.put(task)
# tnsname 
# dblink
# tmptable
# prodtable
# tableowner
# imonth
# position
class TASKINFO:
    def __init__(self,tnsname,dblink,tmptable,prodtable,tableowner,imonth,position , sqltext=''):
        self.tnsname=tnsname;
        self.dblink=dblink;
        self.tmptable=tmptable;
        self.prodtable=prodtable;
        self.tableowner=tableowner;
        self.imonth=imonth;
        self.position=position;
        self.sqltext=sqltext;

    def setTnsname(self,tnsname):
        self.tnsname=tnsname;
    def setDblink(self,dblink):
        self.dblink=dblink;
    def setTmptable(self,tmptable):
        self.tmptable=tmptable;
    def setProdtable(self,prodtable):
        self.prodtable=prodtable;
    def setTableowner(self,tableowner):
        self.tableowner=tableowner;
    def setImonth(self,imonth):
        self.imonth=imonth;
    def setPosition(self,position):
        self.position=position;
    def setSqltext(self,sqltext):
        self.sqltext=sqltext;
    def getTnsname(self):
        return self.tnsname 
    def getDblink(self):
        return self.dblink;
    def getTmptable(self):
        return self.tmptable 
    def getProdtable(self):
        return self.prodtable;
    def getTableowner(self):
        return  self.tableowner;
    def getImonth(self):
        return self.imonth;
    def getPosition(self):
        return self.position;
    def getSqltext(self):
        return self.sqltext;

class ConsumerThread(threading.Thread):
    def __init__(self, task_queue):
        threading.Thread.__init__(self)
        self.task_queue = task_queue
    def run(self):
        thread_name = self.name
        while True:
            next_task = self.task_queue.get()  
            if next_task is None:
                print '%s: Exiting' % thread_name
                break
            #print '%s: %s' % (thread_name, next_task)
            p = next_task 
            print thread_name , ' ' ,p.getTnsname() ,' ',p.getTableowner(),' ', p.getDblink(),' ' , p.getTmptable(),' ' ,p.getProdtable() 
        return



class QueueCheckThread(threading.Thread):
    def __init__(self, task_queue):
        threading.Thread.__init__(self)
        self.task_queue = task_queue
    def run(self):
        thread_name = self.name
        global num  
        while True:
            if num == 3 :
                x = 0 ;
                mylock.acquire() #Get the lock 
                while x < 3:
                    taskQueue.put(None)
                    x = x + 1 
                mylock.release()  #Release the lock
                break ;

            if  task_queue.empty():
                print  'load-new-task'
                task = TASKINFO('tttns','dblink','tmptable','prodtable','tableowner','imonth','position' ,'sqltext')
                task_queue.put(task)
        return


class SetTaskThread(threading.Thread):
    def __init__(self, task_queue):
        threading.Thread.__init__(self)
        self.task_queue = task_queue
    def run(self):
        thread_name = self.name
        global num  
        while True:
            mylock.acquire() #Get the lock 
            num = num+1
            mylock.release()  #Release the lock 
            if num  == 3 :
                 break;
            time.sleep(10)
            print 'set num  = num + 1 &  sleep(1)'
        return

class FundBIllThreadNew(threading.Thread):
    def __init__(self, threadname,taskQueue):
        threading.Thread.__init__(self, name = threadname)
        self.taskQueue=taskQueue
        self.tname = threadname   
    def run(self):
        logger = logging.getLogger('%s'%(self.tname))
        formatter = log.ColoredFormatter('%(name)-12s %(asctime)s %(levelname)-8s %(message)s', '%a, %d %b %Y %H:%M:%S',)  
        file_handler = logging.FileHandler("%s.log"%(self.tname)) 
        file_handler.setFormatter(formatter)  
        stream_handler = logging.StreamHandler(sys.stderr) 
        logger.addHandler(file_handler)  
        logger.addHandler(stream_handler) 
        while not self.taskQueue.empty(): 
            task        = self.taskQueue.get() 
            tns         = task.getTnsname()
            sql         = task.getSqltext()
            towner      = task.getTableowner( )
            prodtable   = task.getProdtable( )
            tmptable    = task.getTmptable( )
            dblink      = task.getDblink( )
            print self.tname,' : ' ,dblink.ljust(15),' ' , prodtable.ljust(25),' ' ,towner 
            logger.info(str(self.tname)+':connect to '+ tns +'success')
            conn = cx_Oracle.connect('devdba','devdba',tns,threaded = True)
            logger.info(str(self.tname)+':connect to'+ tns +'success')
            #print 'thread_name: ',self.tname, conn.version,conn.username ," : "  ,time.ctime()
            logger.info(str(self.tname)+' : '+sql) 
            cur = conn.cursor()
            hischecksql='''
            select count(*) from tradearch.beyond_trade_fund_bill where trade_no=:tradeNo and bill_no=:billNo and to_char(gmt_create,'yyyy-mm-dd hh24:mi:ss')=:chars
            '''
            #prodtab = self.params['user']+'.'+self.params['fndbil']+'@'+self.params['dblink'] 
            #prodtab = towner +'.'+self.params['pfndbil']+'@'+self.params['dblink'] 
            prodtab = towner + '.' + tmptable + '@' + dblink
            tampinsertsql = '''
            insert into tradearch.beyond_trade_fnd_bil_corr_tmp select * from %s where trade_no=:tradeNo and bill_no=:billNo and to_char(gmt_create,'yyyy-mm-dd hh24:mi:ss')=:chars
            '''
            hisinsertsql = tampinsertsql%(prodtab)
            hcon  = getOraConnTnsUP('migrate2','migrate2','tradehis_pri')
            shcur = hcon.cursor()
            ihcur = hcon.cursor()
            try:
                #cur.execute(sql)
                #rs = cur.fetchall()
                #args1 = {}
                print ' '
                #logger.info(str(self.tname)+': FundBIllThreadNew : '+ tns +')  have %s' %len(rs)) 
                #for x in rs : 
                #   args1['tradeNo'] = x[0]
                #   args1['billNo'] = x[1]
                #   args1['chars'] = x[2]
                #   shcur.execute(hischecksql,args1)
                #   my = shcur.fetchall()
                #   t =  my[0][0]
                #   if t == 0 : 
                #        ihcur.execute(hisinsertsql,args1)
                #        logger.info(str(self.tname)+':tuple ('+str(args1)+') OK') 
                #   else : 
                #        #print ---- 
                #        logger.info(str(self.tname)+':tuple ('+str(args1)+') ERROR') 
                #hcon.commit()
            except Exception as e :
                print tns,' ' , e
                logger.info(str(self.tname)+ tns +'  EXcepion : '+str(e)) 
            finally: 
                hcon.commit()
                cur.close()
                conn.close() 
                shcur.close()
                ihcur.close()
                hcon.close()

def help():
    help_str = ''' 
Usage:
    process the trade_fund_bill_0xx error data  in migration .
For example:
    newfb.py -m 201204 
    newfb.py -m 201204 -p 16  
Options include:
  --m      : the migrate month  
  --p      : the number of thread to run this job 
  --help   : help info '''
    return help_str

def main():
     num_consumers = 3
     imonth = 'no'
     leng = len(sys.argv)
     if leng == 3 :
         if sys.argv[1].strip() == '-m' : 
             imonth  = sys.argv[2].strip() 
         else : 
             print '[ERROR] Invalid option. Exit.'
             print help()
             sys.exit()
     elif leng == 5:
          if sys.argv[1].strip() == '-m' : 
               imonth  = sys.argv[2].strip()
          elif sys.argv[1].strip() == '-p' : 
               num_consumers  = int(sys.argv[2].strip())
          else : 
             print '[ERROR] Invalid option. Exit.'
             print help()
             sys.exit()
     
          if sys.argv[3].strip() == '-m' : 
               imonth  = sys.argv[5].strip()
          elif sys.argv[3].strip() == '-p' : 
               num_consumers  = int(sys.argv[4].strip())
          else : 
             print '[ERROR] Invalid option. Exit.'
             print help()
             sys.exit()
         
     else : 
         print '[ERROR] Invalid option. Exit.'
         print help()
         sys.exit()
     
     print  'imonth : ' , imonth
     print  'num_consumers : ' , num_consumers
     try : 
        t = time.strptime(imonth, "%Y%m")
        print 'you are going to process the month  %s \'s exception data !!! ' %(imonth)
     except  Exception , e : 
          print e
          sys.exit()

     meta = getfundbillmeta()
     consumers = [ ConsumerThread( taskQueue, )  for i in xrange(num_consumers) ]
     print '%d workers were Created for the job !!! ' % num_consumers  
     addTaskToQueue(meta,imonth,taskQueue)
     for w in consumers:
        w.start() 
     stt = SetTaskThread(taskQueue)
     stt.start()
     stt.join()
     qck = QueueCheckThread(taskQueue)
     qck.start()
     qck.join()
     for w in consumers:
        w.join()
     
if __name__ == '__main__':
    taskQueue = Queue.Queue() 
    main()
