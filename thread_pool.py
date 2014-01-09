#encoding='utf-8'

import Queue,sys
import threading
from os import makedirs,unlink,sep
from os.path import dirname,exists,isdir,splitext 
from string import replace
import socket
import urllib2
from BeautifulSoup import BeautifulSoup
import urlparse
import re
from time import sleep,strftime

#working thread 
class Worker(threading.Thread):
    worker_count = 0
    timeout = 3
    def __init__(self,visitedQueue,unvisitQueue,logfilename,**kws):
        threading.Thread.__init__(self,**kws)
        self.id = Worker.worker_count
        Worker.worker_count += 1
        #self.setDaemon(True)
        self.logfilename = logfilename
        self.myLockv = threading.RLock()
        self.myLocku = threading.RLock()
        self.myLockfile = threading.RLock()
        self.visitedQueue = visitedQueue
        self.unvisitQueue = unvisitQueue
        self.urlfiltersuffix = ["gif","jpg","png","ico","css","sit","eps","wmf","zip","ppt","mpg","xls","gz","rpm","tgz","mov","exe","jpeg","png","bmp"]
        self.urlfilterscheme = ["file","ftp","mailto"]
        self.start()

    def run(self):
        while True:
            try:
                #callable,args,kwds = self.workQueue.get(timeout = Worker.timeout)
                #res = callable(*args,**kwds)
                self.myLocku.acquire()
                visiturl = self.unvisitQueue.get(timeout=Worker.timeout)
                self.myLocku.release()
                res = self.getPageContent(visiturl)
                print "worker[%2d]: %s" % (self.id,str(res[0]))
                self.myLockv.acquire()
                self.visitedQueue.append(visiturl)
                self.myLockv.release()
                if res[0]=="200":
                    try:
                        filename = self.getFilename(visiturl)
                        fileobject = open(filename,"w")
                        fileobject.write(res[1])
                        fileobject.close()
                        self.myLockfile.acquire()
                        filelog = open(self.logfilename,"a")
                        filelog.write("Succedd download url: %s\n"%visiturl)
                        filelog.close()
                        self.myLockfile.release()
                    except Exception,e:
                        print 'Writing file:' + str(e)
                        self.myLockfile.acquire()
                        filelog = open(self.logfilename,"a")
                        filelog.write("Failed download url: %s\n"%visiturl)
                        filelog.close()
                        self.myLockfile.release()
                        continue 
                    try:
                        soup = BeautifulSoup(res[1])
                        a = soup.findAll("a",{"href":re.compile(".*")})
                    except Exception,e:
                        print 'BeautifulSoup:' + str(e)
                        continue
                    for i in a:
                        if i["href"] != "":
                            abs_url = urlparse.urljoin(visiturl,i["href"])
                            abs_url = self.urlFilter(abs_url)
                            if abs_url != None and abs_url !="" and self.sameHost(visiturl,abs_url):
                                if abs_url not in self.visitedQueue:
                                    self.myLockv.acquire()
                                    self.visitedQueue.append(abs_url)
                                    self.myLockv.acquire()
                                    self.myLocku.acquire()
                                    self.unvisitQueue.put(abs_url)
                                    self.myLocku.release()
                else:
                    self.myLockfile.acquire()
                    filelog = open(self.logfilename,"a")
                    filelog.write("Failed download url: %s\n"%visiturl)
                    filelog.close()
                    self.myLockfile.release()
            except Queue.Empty:
                break
            except:
                print 'worker[%2d]' % self.id, sys.exc_info()[:2]
                raise

    
    def getPageContent(self,url):
        try:
            socket.setdefaulttimeout(100)
            req = urllib2.Request(url)
            req.add_header('User-agent','Mozilla/4.0 (compatible; MSIR 6.0; Windows NT 5.1')
            sleep(Worker.timeout)
            response = urllib2.urlopen(req)
            coding =  response.headers.getparam("charset")
            if coding is None:
                page = response.read()
            else:
                page = response.read()
                page  = page.decode(coding).encode('utf-8')
            return '200',page
        except Exception,e:
            return str(e),None
    def getFilename(self,url):
        parsedurl = urlparse.urlparse(url)
        path = parsedurl[1] + parsedurl[2]
        ext = splitext(path)
        if ext[1] == "":
            if path[-1] == "/":
                path = path + url.replace("/","_")+".html"
            else:
                path = path[0:path.rfind("/")] + "/" + url.replace('/','_')+".html"
        else:
            if path.rfind('/')==-1:
                path = path + '/' + url.replace('/','_')+".html"
            else:
                path = path[0:path.rfind("/")] + "/" + url.replace('/','_')+".html"
        dir = dirname(path)
        if sep != "/":
            dir = replace(dir,"/",sep)
        if not isdir(dir):
            if exists(dir):
                unlink(dir)
            makedirs(dir)
        return path
    def urlFilter(self,urltest):
        parsedurl = urlparse.urlparse(urltest)
        if parsedurl.scheme not in self.urlfilterscheme:
            if parsedurl.path.split(".")[-1] not in self.urlfiltersuffix:
                return urltest
        return None
    def sameHost(self,url,urltest):
        parsedurl1 = urlparse.urlparse(url)
        parsedurl2 = urlparse.urlparse(urltest)
        if parsedurl1.hostname == parsedurl2.hostname:
            return True
        else:
            return False

class WorkerManager:
    def __init__(self,num_of_workers=10,timeout = 2):
        self.visitedQueue = []
        self.unvisitQueue = Queue.Queue()
        self.workers = []
        self.timeout = timeout
        self.logfilename = './log' + str(strftime('%Y%m%d%H%M%S')) + '.xml'
        self._recruitThreads(num_of_workers)
        
        
    def _recruitThreads(self,num_of_workers):
        for i in range(num_of_workers):
            worker = Worker(self.visitedQueue,self.unvisitQueue,self.logfilename)
            self.workers.append(worker)

    def wait_for_complete(self):
        while len(self.workers):
            worker = self.workers.pop()
            worker.join()
            if worker.isAlive() and not self.unvisitQueue.empty():
                self.workers.append(worker)
        print self.unvisitQueue.qsize()
        print "All jobs are completed."

    #def add_job(self,callable,*args,**kwds):
        #self.workQueue.put( (callable,args,kwds) )

    #def get_result(self,*args,**kwds):
        #return self.resultQueue.get(*args,**kwds)

    def add_unvisitLinks(self,url):
        self.unvisitQueue.put(url)
    def add_visitedLinks(self,url):
        self.visitedQueue.append(url)
