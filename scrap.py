from bs4 import BeautifulSoup
import threading
import urllib2
import redis

base_url    = "https://www.mustakbil.com"
#base_url    = "https://www.mustakbil.com/job/110036"
http        = "http"
queue       = []
queue.append("https://www.mustakbil.com")
redis       = redis.StrictRedis(host='localhost', port=6379, db=0)
key   = "mustakbil"


def get_in_links(page_url, links):

    in_links    = []
    for link in links:
        href    = link.get("href")

        if href and http not in href:
            href = "%s%s" % (base_url, href) if href.startswith("/") else "%s/%s" % (page_url, href)
 
            if "https://www.mustakbil.com/events/" not in href and not redis.exists("%s:%s" % ("queue", href)):
#                print "url %s has been queued" % (href)
                redis.set("%s:%s" % ("queue", href), 1)
                in_links.append(href)
#            else:
#               print "%s has already been queued" % (href)


    in_links.reverse()
    return in_links


def parse(url, html):


    dict    = {}
    try:

        title   = html.find("h1").text

        table   = html.find("table", {"class" : "table table-condensed"})
        tr      = table.find("tr")
        cols    = tr.find_all("td")
        col     = cols[1]
        a       = col.find("a")
        catrogy = a.text

        rows        = table.find_all("td", {"class" : "col-md-8"})
        company     = rows[0].text.strip()
        city        = rows[1].text.strip()


        dict["title"]   = title
        dict["catrogy"] = catrogy
        dict["company"] = company
        dict["city"]    = city
        dict["url"]     = url

    except:
        pass

    return dict


def init(queue):


    count = 0
    while count< 5000:
        count +=1
        try:
            
            header      = {'User-Agent': 'Mozilla/5.0'}
            url         = queue.pop()
#            print count

            if not redis.exists("%s:%s" % (key, url)):

#                print "crawling %s ... " % (url)

                req         = urllib2.Request(url, headers=header)
                page        = urllib2.urlopen(req)
                soup        = BeautifulSoup(page)
                links       = soup.find_all("a")
                ret_links   = get_in_links(url, links)
                queue       = ret_links + queue
                redis.set("%s:%s" % (key, url), 1)
                redis.sadd(key, url)

                dict        = parse(url, soup)
                if dict:
                    print dict
            else:
#               print "%s already crawled ....." % (url)
                pass

        except:
            print "error ........."
            pass

init(queue)
print queue
