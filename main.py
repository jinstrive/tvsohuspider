import redisoper
import parsehtml
import sqlconn
import time
import gevent
import sys
# max_urls = 5000

def url_producer(max_urls=10000):
    roper = redisoper.SpiderRedisOper()
    while True:
        pop_url = roper.pop_for_generate()
        if not pop_url:
            roper.insertUrl("http://tv.sohu.com/")
            continue
        parser = parsehtml.HtmlParser(pop_url)
        links = parser.get_all_href()
        for link in links:
            ret,exist_num = roper.insertUrl(link)
            if exist_num >= max_urls:
                return
            
            
def url_customer():
    roper = redisoper.SpiderRedisOper()
    sqloper = sqlconn.DataPersist()
    while True:
        pop_url = roper.pop_for_analyze()
        if not pop_url:
            time.sleep(5)
            if roper.show_count_a() <= 0:
                return
            continue
        parser = parsehtml.HtmlParser(pop_url)
        mess = parser.get_useful_mess()
        sqloper.insert(mess)
        
def main(producer=3,customer=3,max_urls=10000):
    threads = []
    if producer<=0 or customer<=0:
        print "error params"
        return
    for i in xrange(0,producer):
        tr = gevent.spawn(url_producer,max_urls)
        threads.append(tr)
        
    for i in xrange(0,customer):
        tr = gevent.spawn(url_customer)
        threads.append(tr)
    
    gevent.joinall(threads)
    
if __name__ == '__main__':
        argv = sys.argv
        if len(argv) == 4:
            main(max_urls=argv[1],producer=argv[2],customer=argv[3])
        else:
            main()