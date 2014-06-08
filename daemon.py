#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys, os, time, atexit
from signal import SIGTERM
import re
import sys,os
import math
import time
import commands,string
from signal import SIGTERM
from optparse import OptionParser
import ConfigParser
import logging
import threading
import multiprocessing
import Queue
class Daemon:
    """
    A generic daemon class.

    Usage: subclass the Daemon class and override the _run() method
    """
    def __init__(self,pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
    def _daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        #脱离父进程
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
        #脱离终端
        os.setsid()
        #修改当前工作目录
        os.chdir("/")
        #重设文件创建权限
        os.umask(0)
        #第二次fork，禁止进程重新打开控制终端
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        #重定向标准输入/输出/错误
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
        #注册程序退出时的函数，即删掉pid文件
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile,'w+').write("%s\n" % pid)
    def delpid(self):
        os.remove(self.pidfile)
    def start(self):
        """
        Start the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
        if pid:
            message = "pidfile %s already exist. Daemon already running?\n"
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)
        # Start the daemon
        self._daemonize()
        self._run()
    def stop(self):
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return # not an error in a restart
        # Try killing the daemon process
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)
    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start()
    def _run(self):
        """
        You should override this method when you subclass Daemon. It will be called after the process has been
        daemonized by start() or restart().
        """

def TimeFormat(format = '%Y-%m-%d %X'):
    '''
    convert a ISO format time to second
    from:23123123 to 2006-04-12 16:46:40
    '''
    ISOTIMEFORMAT = format
    return  time.strftime( ISOTIMEFORMAT, time.localtime( time.time() ) )

def initLogger():
    logger = logging.getLogger("ruiaylin")
    formater = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s","%Y-%m-%d %H:%M:%S")
    file_handler = logging.FileHandler("ruiaylin.log")
    file_handler.setFormatter(formater)
    stream_handler = logging.StreamHandler(sys.stderr)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO)
    return logger

def addTaskToQueue(x):
    nameprex = 'task'
    for fb in xrange(x) :
       task = TASKINFO(nameprex+str(fb),"desc"+str(fb),"stepx %s"%(fb) )
       taskQueue.put(task)
    logger.info('init %s  task !!!! '%(x))

#task object
class TASKINFO:
    def __init__(self,taskName,taskDesc,steps):
        self.taskName = taskName
        self.taskDesc = taskDesc
        self.steps    = steps
    def setTaskName(self,name):
        self.taskName = name
    def getTaskName(self):
        return self.taskName
    def setTaskDesc(self,tdesc):
        self.taskDesc = tdesc
    def getTaskDesc(self):
        return self.taskDesc
    def setSteps(self,steps):
        self.steps = steps
    def getSteps(self):
        return self.steps

class MyDaemon(Daemon):
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
           Daemon.__init__(self,pidfile, stdin , stdout , stderr )
           self.dates = []
    def _run(self):
        logger.info(' demon process started !!!! ')
        while True:
            time.sleep(2)
            logger.info('demon process started !!!! %s '%(TimeFormat()))
            fsdt = TimeFormat()
            self.dates.append(fsdt)
            logger.info(' fsdt = %s '%(fsdt))

class CheckingTask(Daemon):
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
           Daemon.__init__(self,pidfile, stdin , stdout , stderr )
    def _run(self):
        logger.info('demon process started !!!! ')
        while True:
            addTaskToQueue(30)
            logger.info('create three ConsumerThread for cheking !!!! ')
            consumers = [ ConsumerThread('custthread-'+str(i))  for i in xrange(3) ]
            for w in consumers:
                w.start()
            logger.info('ConsumerThreads are  starting !!!! ')
            time.sleep(10)
            logger.info(' sleep  = 300 %s '%(TimeFormat()))

class ConsumerThread(threading.Thread):
    def __init__(self, threadname):
        threading.Thread.__init__(self, name = threadname)
    def run(self):
        thread_name = self.name
        while not taskQueue.empty():
            next_task = taskQueue.get()
            #print '%s: %s' % (thread_name, next_task)
            p = next_task
            logger.info("thread_name : %s , TaskName = %s , TaskDesc = %s [%s]" %(thread_name ,p.getTaskName() ,p.getTaskDesc(),TimeFormat()))
        logger.info("thread_name  %s exited  ----- "%(thread_name))

if __name__ == "__main__":
    taskQueue = Queue.Queue()
    logger = initLogger()
    daemon = CheckingTask('/tmp/tbs-checking-process.pid')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)
