#!/home/oracle/dbapython/bin/python
# author     : ruichao.lin
# contract   : ruiaylin@gmail.com
# func       : oracle database switchover
# lang       : python
# version    : python 2.7
# date       : 2013-09
# for        : GNU - linux
# encoding   : UTF-8
# -*- coding: gbk -*-
"""
ScriptName  : lrcODBswitchover.py
Author      : ruiaylin
Create Date : 2013-09-01
Function    : oracle dataguard switchover tools
oracle 主备切换脚本，初级版本，写的比较早了，很多地方需要修改，
各位可以根据自己需要进行修改扩孔，但要标明第一作者 哈哈
"""
import re
import sys,os
import math
import time,signal
import  string,commands
from signal import SIGTERM
import cx_Oracle
import string
from optparse import OptionParser
import ConfigParser
from termcolor import colored
import logging


# functions
error_count =  0 ;
redoap_flag =  0 ;

#output uniform
def outPut(a,b):
    print " ",a.ljust(25)," :  " , b

def checkprint(tabcount,preinfo,infor,warning,value):
    tabsize = 2
    preinfo=colored(preinfo,'cyan')
    infor=colored(infor,'cyan')
    if warning.upper() == 'OK' :
        warning=colored(warning,'green')
    else :
        warning=colored(warning,'red')
    numbofBlank=tabcount*tabsize
    for x in range(1 ,numbofBlank):
        print ' ',
    print  preinfo.ljust(25),':',infor.ljust(34) ,'  value : [ ', value.ljust(20), ' ]   check result : [ ',warning.ljust(5),' ]'


def normalprint(tabcount,preinfo):
    tabsize = 2
    preinfo=colored(preinfo,'cyan')
    numbofBlank=tabcount*tabsize
    print " "
    for x in range(1 ,numbofBlank):
        print ' ',
    print ' >>',
    print  preinfo.ljust(40)

def dictprint(tabcount,dict):
    tabsize = 15
    numbofBlank=tabcount*tabsize
    for i in range(1 ,numbofBlank):
        print ' ',
    print colored('=='*29 , 'cyan')
    for x in dict :
        for i in range(1 ,numbofBlank):
           print ' ',
        print colored('==  ','cyan') ,colored(' >>','green'),
        print  '   ',str(x).ljust(15),' : ' ,str(dict[x]).ljust(20),colored('  ==','cyan')
    for i in range(1 ,numbofBlank):
        print ' ',
    print colored('=='*29 , 'cyan')

def getoracleconntns(tns):
    conn = cx_Oracle.connect('baiqiao/ali88@%s'%tns ,mode=cx_Oracle.SYSDBA )
    #conn = cx_Oracle.connect('/', mode=cx_Oracle.SYSDBA)
    return conn


def getoracleconntnspa(tns):
    conn = cx_Oracle.connect('baiqiao/ali88@%s'%tns ,mode = cx_Oracle.SYSDBA | cx_Oracle.PRELIM_AUTH)
    #conn = cx_Oracle.connect('/', mode=cx_Oracle.SYSDBA)
    return conn

# get standby database tns  &  ip &
def get_sandby_tns(priconn):
    sql = ''' select value from v$parameter where name = 'log_archive_dest_2'
    '''
    global error_count
    cur = priconn.cursor()
    normalprint(1,'checking standby tns #######################################get_sandby_ips()#### ')
    tnsstb='NA'
    try :
        cur.execute(sql)
        rs = cur.fetchall()
        tnsstb = rs[0][0].split()[0].split('=')[1]
        cur.close()
    except Exception , e:
        print e

    return  tnsstb

def check_tns_get_infos(tns):
    infos={}
    tnsCommand = 'tnsping %s' %tns
    commandOutput = commands.getoutput(tnsCommand)
    findResults = string.split(commandOutput, "\n")
    isOK = findResults[-1]
    if isOK.split()[0] == 'OK' :
        sip = findResults[10].split('(')[5].split(')')[0].split('=')[1].strip()
        sport = findResults[10].split('(')[6].split(')')[0].split('=')[1].strip()
        SERVICE_NAME = findResults[10].split('(')[8].split(')')[0].split('=')[1].strip()
        infos['ip'] = sip
        infos['port'] = sport
        infos['service_name'] = SERVICE_NAME
        checkprint(2,'check TNSNAMES','tnsping','OK',tns)
        dictprint(1,infos)
    else :
        checkprint(2,'check TNSNAMES','tnsping','ERROR',tns)
        global error_count
        error_count = error_count + 1
    return  infos


# check primary database information  &  status
def check_primary_infos(conn):
    normalprint(1,'checking primary infos #######################################check_primary_infos()#### ')
    sql1 = ''' select value from v$parameter where name = 'log_archive_dest_state_2' '''
    sql2 = ''' select database_role , switchover_status from v$database  '''
    cur = conn.cursor()
    global error_count
    switchss = ['TO STANDBY','SESSION ACTIVE']
    try :
        cur.execute(sql1)
        rs = cur.fetchall()
        lads2 = rs[0][0]
        if lads2.upper() == 'ENABLE' :
                        checkprint(2,'CHECKING PRIMARY','log_archive_dest_state_2','OK',lads2)
        else :
            checkprint(2,'CHECKING PRIMARY','log_archive_dest_state_2','ERROR',lads2)
            error_count = error_count + 1
        cur.execute(sql2)
        rs1 = cur.fetchall()
        db_role,switch_state = rs1[0][0], rs1[0][1]
        if db_role.upper() == 'PRIMARY' :
            checkprint(2,'CHECKING PRIMARY','database_role','OK',db_role)
        else :
            checkprint(2,'CHECKING PRIMARY','database_role','ERROR',db_role)
            error_count = error_count + 1

        if switch_state.upper() in switchss :
            checkprint(2,'CHECKING PRIMARY','switchover_status','OK',switch_state)
        else :
            checkprint(2,'CHECKING PRIMARY','switchover_status','ERROR',switch_state)
            error_count = error_count + 1

    except Exception , e:
        print e


# check standby database information  &  status
def check_standby_infos(conn):
    normalprint(1,'checking standby infos #######################################check_standby_infos()#### ')
    sql1 = ''' select PID  from  v$managed_standby where PROCESS = 'MRP0'  '''
    sql2 = ''' select database_role , open_mode  from v$database  '''
    openModes = ['MOUNTED','READ ONLY','READ ONLY WITH APPLY']
    cur = conn.cursor()
    global error_count
    try :
        cur.execute(sql1)
        rs = cur.fetchall()
        if len(rs)  != 0 :
                checkprint(2,'CHECKING STANDBY','mrp0 process','OK','mrp')
        else :
            checkprint(2,'CHECKING STANDBY','mrp0 process','ERROR','mrp')
            error_count = error_count + 1

        cur.execute(sql2)
        rs1 = cur.fetchall()
        db_role,open_mode = rs1[0][0], rs1[0][1]
        if db_role.upper() == 'PHYSICAL STANDBY' :
            checkprint(2,'CHECKING STANDBY','database_role','OK',db_role)
        else :
            checkprint(2,'CHECKING STANDBY','database_role','ERROR',db_role)
            error_count = error_count + 1

        if open_mode.upper() in openModes :
            checkprint(2,'CHECKING STANDBY','open_status','OK',open_mode)
        else :
            checkprint(2,'CHECKING STANDBY','open_status','ERROR',open_mode)
            error_count = error_count + 1

    except Exception , e:
        print e


#check
def shutdownDB(conn):
    # need to connect as SYSDBA or SYSOPER
    # first shutdown() call must specify the mode, if DBSHUTDOWN_ABORT is used,
    # there is no need for any of the other steps
    conn.shutdown(mode = cx_Oracle.DBSHUTDOWN_IMMEDIATE)
    # now close and dismount the database
    cursor = conn.cursor()
    cursor.execute("alter database close normal")
    cursor.execute("alter database dismount")
    # perform the final shutdown call
    conn.shutdown(mode = cx_Oracle.DBSHUTDOWN_FINAL)

def startupDB(tns):
    #------------------------------------------------------------------------------
    # DatabaseStartup.py
    #   This script demonstrates starting up a database using Python. It is only
    # possible in Oracle 10g Release 2 and higher. The connection used assumes that
    # the environment variable ORACLE_SID has been set.
    #-----------------------------------------------------------------------------
    # the connection must be in PRELIM_AUTH mode
    #connection = cx_Oracle.connect("",            mode = cx_Oracle.SYSDBA | cx_Oracle.PRELIM_AUTH
    connection = getoracleconntnspa(tns)
    connection.startup()
    # the following statements must be issued in normal SYSDBA mode
    connection = getoracleconntns(tns)
    cursor = connection.cursor()
    cursor.execute("alter database mount")
    cursor.execute("alter database open")


# time out
class TimedOutExc(Exception):
    def __init__(self, value = "Timed Out"):
        self.value = value
    def __str__(self):
        return repr(self.value)

def TimedOutFn(f, timeout, *args):
    def handler(signum, frame):
        raise TimedOutExc()

    signal.signal(signal.SIGALRM, handler)
    signal.alarm(timeout)

    return f(*args)

    signal.arlam(0)

def timed_out(timeout, *args):
    def decorate(f):
        def handler(signum, frame):
            raise TimedOutExc()

        def new_f(*args):
            signal.signal(signal.SIGALRM, handler)
            signal.alarm(timeout)
            return f(*args)
            signal.alarm(0)
        new_f.func_name = f.func_name
        return new_f
    return decorate

# check redo apply
@timed_out(240)
def checkcounts(conn,counts):
     ssql   =  ''' select max(sequence#) from v$archived_log where applied = 'YES'  '''
     cur = conn.cursor()
     while True :
         cur.execute(ssql)
         srs = cur.fetchall()
         sapid = srs[0][0]
         print sapid
         if sapid == counts:
             break ;
         time.sleep(5)
     return 1 ;
# check redo apply
@timed_out(240)
def checkcountsn(conn,pconn):
     ssql   =  ''' select max(sequence#) from v$archived_log where applied = 'YES'  '''
     psql   = ''' select max(sequence#) from v$archived_log  '''
     cur = conn.cursor()
     pcur= pconn.cursor()
     timec = 0;

     while True :
         cur.execute(ssql)
         srs = cur.fetchall()
         sapid = srs[0][0]
         pcur.execute(psql)
         prs=pcur.fetchall()
         papid = prs[0][0]
         sys.stdout.write(colored('\tWaiting time : ','green')+colored('%s s \r'%timec,'red'))
         sys.stdout.flush()
         if sapid == papid :
             break ;
         time.sleep(1)
         timec=timec+1
     sys.stdout.write('\n')
     return 1 ;


def check_redo_applied(pconn , sconn):
    normalprint(1,'checking redo  applied  #######################################check_redo_applied()#### ')
    #psql1   = ''' select max(sequence#) from v$archived_log where DEST_ID = 2 and applied = 'YES'  '''
    psql1   = ''' select max(sequence#) from v$archived_log  '''
    pactsql =  ''' alter system switch logfile '''
    ssql1   =  ''' select max(sequence#) from v$archived_log where applied = 'YES'  '''
    pcur  =   pconn.cursor()
    scur  =   sconn.cursor()
    global error_count
    global redoap_flag
    try :
        pcur.execute(psql1)
        scur.execute(ssql1)
        prs = pcur.fetchall()
        srs = scur.fetchall()
        papid = prs[0][0]
        sapid = srs[0][0]
        pcur.execute(pactsql)
        time.sleep(1)
        pcur.execute(pactsql)
        print colored('\tChecking','green'),' : '
        pcur.execute(pactsql)
        try:
           redoap_flag = checkcountsn(sconn,pconn)
        except TimedOutExc:
            print '5 minites have passed away ,please check the redo apply'
        if redoap_flag  == 0 :
            checkprint(2,'CHECKING REDO ','REDO APPIED','ERROR','DELAY BW PRI&STB')
            error_count = error_count + 1
        else :
            checkprint(2,'CHECKING REDO ','REDO APPIED','OK','REDO APPLIED OK ')
    except Exception , e :
        print e
    finally:
        pcur.close()
        scur.close()
#check parameters
def check_parameters(pconn , sconn):
    normalprint(1,'checking parameter #######################################check_parameters()#### ')
    sql  =  '''select NAME,VALUE from v$parameter where ISDEFAULT = 'FALSE'  '''
    pcur  =   pconn.cursor()
    scur  =   sconn.cursor()
    tmps = {}
    dftmps = {}
    dfcount = 0
    global error_count
    try :
        pcur.execute(sql)
        scur.execute(sql)
        prs = pcur.fetchall()
        srs = scur.fetchall()
        for x in prs :
            tmps[x[0]] = x[1]
        normalprint(2,'CHECKING PARAMS  [PRI] count : %s  '%len(tmps))
        for x1 in srs :
            if tmps[x1[0]] !=  x1[1]:
                 dftmps[x1[0]] = x1[1]
                 dfcount = dfcount + 1


        if dfcount > 0 :
            normalprint(2,'SOME DIFF PARAMS  : '  )
            error_count = error_count + 1
            for df in dftmps :
                print '\t\t',df ,' :  [PRI] ',tmps[df],' [STB] ',dftmps[df]
        else:
            checkprint(2,'CHECKING PARAMS ','DIFF PARAMS','OK','PARAMS OK ')


    except Exception , e :
        print e
    finally:
        pcur.close()
        scur.close()

###############################################
def tnsreplace(ip,old,new):
    try:
        ip='10.209.172.14'
        shs = pxssh.pxssh()
        shs.login (ip, 'oracle', '')
        shs.sendline ('echo $ORACLE_HOME ')
        shs.prompt()
	    xs =  shs.before.split('\n')
	    ohome = xs[1]
	    listfile = xs[1].strip()+'/network/admin/listener.ora'
        os.system('ssh %s \"sed -i \'s/%s/%s/\' %s\" ' %(ip,old,new,listfile))
    except pxssh.ExceptionPxssh, e:
        print "pxssh failed on login."
        print str(e)

def exchangeips(ip,old,new):
    try:
        ip='10.209.172.14'
        shs = pxssh.pxssh()
        shs.login (ip, 'oracle', '')
        shs.sendline ('echo $ORACLE_HOME ')
        shs.prompt()
        xs =  shs.before.split('\n')
        ohome = xs[1]
        tnsfile = xs[1].strip()+'/network/admin/tnsnames.ora'
        tmps1= 'as19oq90823423ssw'
        tmps2= 'asdfawewerweweww'
        os.system('ssh %s \"sed -i \'s/%s/%s/g\' %s\" ' %(ip,old,tmps1,tnsfile))
        os.system('ssh %s \"sed -i \'s/%s/%s/g\' %s\" ' %(ip,new,tmps2,tnsfile))
        os.system('ssh %s \"sed -i \'s/%s/%s/g\' %s\" ' %(ip,tmps1, new,tnsfile))
        os.system('ssh %s \"sed -i \'s/%s/%s/g\' %s\" ' %(ip,tmps2,old,tnsfile))
    except pxssh.ExceptionPxssh, e:
       print "pxssh failed on login."
       print str(e)

def main_check():
    print "##############\t The switchover's check is starting !!!"
    tnspri = 'acctrans_pri'
    pconn  = getoracleconntns(tnspri)
    tnsstb = get_sandby_tns(pconn)
    sconn  = getoracleconntns(tnspri)
    pritnsinfos = check_tns_get_infos(tnspri)
    stbtnsinfos = check_tns_get_infos(tnsstb)
    sconn  = getoracleconntns(tnsstb)
    check_primary_infos(pconn)
    check_standby_infos(sconn)
    check_parameters(pconn , sconn)
    check_redo_applied(pconn , sconn)

    print " "
    if error_count == 0 :
         print colored("##############\t The switchover's check result successing  !!!",'green')
    else :
         print colored("##############\t The switchover's check result erroring    !!!",'red')
    sconn.close()
    pconn.close()
main_check()
