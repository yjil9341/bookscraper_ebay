import requests
import multiprocessing
import settings
from bs4 import BeautifulSoup

from multiprocessing import Pool, Lock, Manager
import multiprocessing
import os
import random
from functools import partial
import sqlite3
import time

def get_listingids(url,sharedproxs):
    try:
        proxy = random.choice(sharedproxs[0])
        r = requests.get(url, headers = settings.headers ,proxies=proxy)
        html = r.content
    except:
        print "Something is wrong making request + getting html. Skipping..."
        return []

    soup = BeautifulSoup(html,'html.parser')
    results = soup.find_all('li',attrs={'class':'sresult'})
    if results == []:
        print "There is potentially an error, or exceeded 10,000 limit. Skipping ..."
        return []

    
    listingids = []
    count = 0
    for result in results:
        count += 1
        listingid = result.get('listingid')
##        titletag = result.find('h3',attrs={'class':'lvtitle'})
##        title = unicode(titletag.find('a').get('title')).encode('ascii','ignore')
        listingids.append(listingid)
    if count < 200:
        f = open('testinghtml.html','w')
        f.write(html)
        f.close()
    else: pass

                
    l = sharedproxs[1]
    l.acquire()
    if os.path.isfile('database/ebay.sqlite') == False:
        conn = sqlite3.connect('database/ebay.sqlite')
        cur = conn.cursor()
        ##Introduces Some kind of concurrency enabled mode. 
        cur.execute('PRAGMA journal_mode=WAL')
        cur.executescript('''

            Create TABLE EbayScrape (
            
            listingid TEXT UNIQUE 
            
            );
            ''')
    else:
        conn = sqlite3.connect('database/ebay.sqlite')
        cur = conn.cursor()
        ##Introduces Some kind of concurrency enabled mode
        cur.execute('PRAGMA journal_mode=WAL')
        for listingid in listingids:
            cur.execute('''INSERT OR REPLACE INTO EbayScrape (listingid)
                        VALUES (?)''', (listingid,))
    conn.commit()
    conn.close()
    l.release()

    print "Success! {} len(html) {} , listintids saved {}".format(url, len(html),count)
    return listingids


                
if __name__ == '__main__':


    ## Assumes, include itemid, price, no picture, 200 listings per page, less than 4 dollars
    baseurl = 'http://www.ebay.com/sch/Textbooks-Education/2228/m.html?_nkw&_armrs=1&_udlo&_udhi=4&_ssn=betterworldbookswest&_ipg=200&_dcat=2228&_sop=15&rt=nc'

    condition_a = '&LH_ItemCondition=6000'
    condition_g = '&LH_ItemCondition=5000'
    condition_vg = '&LH_ItemCondition=4000'
    conditions = [ condition_vg, condition_g , condition_a ]


    format_ns = '&Format=%21'
    format_p = '&Format=Paperback'
    format_h = '&Format=Hardback'
    formats = [ format_h , format_p , format_ns ] 
    pagenum_base = '&_pgn='
    maxpage = 49 ##can only scrape 10,000 results per filter, 200 results per page

    rooturls = []
    for condition in conditions:
        for binding in formats:
            rooturl = baseurl + condition + binding 
            print rooturl
            rooturls.append(rooturl)
    urls = []
    for rooturl in rooturls:
        url = rooturl
        r = requests.get(url , headers = settings.headers)
        html = r.content
        soup = BeautifulSoup(html,'html.parser')
        numbooks = soup.find('span',attrs={'class':'rcnt'})
        numbooks = int(numbooks.string.replace(',',''))
        print len(html)

        ## Can only scrape up to 49 pages.
        ## Find number of pages existing,
        ## comapre it against maxpage, then assign number of pages to crawl programatically
        
        numpages  = numbooks/200
        print numpages
        if numpages < maxpage: numpages = numpages + 1
        else: numpages = maxpage + 1
        for pagenum in range(1,numpages):
            urlsub = rooturl + pagenum_base + str(pagenum)
            urls.append(urlsub)

    print len(urls)

    print "Starting Pool Process ... "
    start = time.time()
    proxs = settings.proxy_paid()
    manager = Manager()
    freshproxies = manager.list(proxs)
    manager2 = Manager()
    l = manager2.Lock()
    partial_get_listingids = partial(get_listingids,sharedproxs = (freshproxies,l))

    p = Pool(4)
    result = p.map(partial_get_listingids, urls, chunksize = 1)
    p.close()
    p.join() 
    print 'Runtime: %ss' % (time.time()-start)
    print result






