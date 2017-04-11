import threading
import requests
from bs4 import BeautifulSoup
import Queue
import sqlite3
import time

TOTAL = 950
URL = 'http://m.mmjpg.com/mm/'
dbpath = './mmjpg.db'


def get_page(url):
    html = requests.get(url).content
    bso = BeautifulSoup(html,'html.parser')
    contentpage = bso.find('div',{'class':'contentpage'})
    total = int(contentpage.text[contentpage.text.find('/')+1:contentpage.text.find(')')])
    urls = [url+'/%d'%i for i in range(1,total+1)]
    return urls

def worker_page(no_q,page_q):
    while no_q.qsize() > 0:
        no = no_q.get(timeout=1)
        try:
            pages = get_page(URL+str(no))
        except Exception:
            continue
        print("Run in %s" % threading.currentThread().getName())
        for page in pages:
            page_q.put(page)


def get_img(url):
    mm_no = url.split('/')[-2]
    html = requests.get(url).content
    bso = BeautifulSoup(html,'html.parser')
    content = bso.find('div',{'class':'content'})
    src = content.img.attrs['src']
    src = mm_no+'|'+src
    return src

def worker_img(page_q,img_q):
    time.sleep(5)
    while page_q.qsize() > 0:
        print("Run in %s" % threading.currentThread().getName())
        page = page_q.get(timeout=40)
        try:
            img = get_img(page)
        except Exception:
            continue
        img_q.put(img)


def save(url):
    split = url.find('|')
    mm_no = url[:split]
    src = url[split+1:]
    sql = '("%s",%d)'%(src,int(mm_no))
    return sql

def execute(sqls):
    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
    values = ','.join(sqls)
    sql = 'insert into imgurl(url,mm_no)values %s;'% values
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()

def worker_save(img_q):
    time.sleep(10)
    sqls = []
    while True > 0:
        print("Run in %s" % threading.currentThread().getName())
        img = img_q.get(timeout=100)
        sqls.append(save(img))
        if len(sqls)> 90:
            execute(sqls)
            sqls = []
    print("%s exited." % threading.currentThread().getName())

def download(url):
    name = url[url.find('//')+2:].replace('/','-')
    content = requests.get(url).content
    with open('./imgs/'+name, 'wb') as f:
        f.write(bytes(content))

def worker_download(img_q):
    time.sleep(10)
    while img_q.qsize()>0:
        print("Run in %s" % threading.currentThread().getName())
        url = img_q.get(timeout=100)
        try:
            download(url)
        except Exception:
            img_q.put(url)
            time.sleep(10)
            continue

def worker_fetch(img_q,res):
    for r in res:
        print("Run in %s" % threading.currentThread().getName())
        img_q.put(r[0])

def get_fetch():
    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
    cur.execute('select url from imgurl;')
    fetch = cur.fetchall()
    cur.close()
    conn.close()
    return fetch

def main():
    '''
    # 下载imgurl
    no_q = Queue.Queue(maxsize=TOTAL+1)
    img_q = Queue.Queue(maxsize=(TOTAL+1)*10)
    page_q = Queue.Queue(maxsize=(TOTAL+1)*10)


    for i in range(TOTAL):
        no_q.put(i)

    threadlist = []
    for i in range(20):
        threadlist.append(threading.Thread(target=worker_page,name='worker_page%d'%i,args=(no_q,page_q,)))
    for i in range(20):
        threadlist.append(threading.Thread(target=worker_img,name='worker_img%d'%i,args=(page_q,img_q,)))
    threadlist.append(threading.Thread(target=worker_save,name='worker_save%d'%1,args=(img_q,)))
    '''

    # 下载图片到磁盘
    img_q = Queue.Queue(maxsize=1000)
    fetch = get_fetch()

    threadlist = []
    threadlist.append(threading.Thread(target=worker_fetch,name='worker_fetch',args=(img_q,fetch,)))
    for i in range(50):
        threadlist.append(threading.Thread(target=worker_download,name='worker_download%d'%i,args=(img_q,)))






    for thread in threadlist:
        thread.start()

    for thread in threadlist:
        thread.join()




if __name__ == '__main__':
    main()
