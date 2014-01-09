
#encoding='utf-8'

import thread_pool

def main():
    url = 'http://www.ict.ac.cn'
    wm = thread_pool.WorkerManager(10)
    for i in range(10):
        wm.add_unvisitLinks(url)
    wm.wait_for_complete()
    print 'ending'



if __name__=="__main__":
    main()
