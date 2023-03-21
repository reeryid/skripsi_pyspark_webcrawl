from bs4 import BeautifulSoup
from pyspark import SparkContext, SparkConf
import findspark
import requests
import pandas as pd

findspark.init()
findspark.find()

conf = SparkConf().setAppName("WebCrawler").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf)

done_crawl=[]

def get_content(url):
    try:
        page = requests.get(url, verify=True, timeout=15)
    except requests.exceptions.HTTPError:
        print("URL Tidak bisa diakses :",url)
        pass
    except requests.exceptions.Timeout:
        print("Timeout koneksi ke :", url)
        pass
    except requests.exceptions.ConnectionError:
        print("Koneksi Error ke :", url)
        pass
    else:
        soup = BeautifulSoup(page.text, 'lxml')
        getteks = soup.get_text()
        if "lombok" in getteks.lower():
            print("Done crawl :",url)
            return getteks 
        else:
            print("tidak ada keyword")
        

def get_links(url):
    try:
        page = requests.get(url, verify=True, timeout=15)
    except requests.exceptions.HTTPError:
        print("URL Tidak bisa diakses :",url)
        pass
    except requests.exceptions.Timeout:
        print("Timeout koneksi ke :", url)
        pass
    except requests.exceptions.ConnectionError:
        print("Koneksi Error ke :", url)
        pass
    else:
        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'lxml')
        getteks = soup.get_text()
        links=[]
        if "lombok" in getteks.lower():
            done_crawl.append(url)
            for link_baru in soup.find_all('a',href=True):
                    if("http" in link_baru['href']):
                        if link_baru.get("href") not in done_crawl:
                            if len(links) < 30:
                                links.append(link_baru.get("href"))
            print("done :",url)
            return links
        else:
            print("tidak ada keyword")


seed_urls = ['https://radarlombok.co.id/','https://www.suara.com/tag/lombok','https://lombokpost.jawapos.com/','https://www.detik.com/search/searchall?query=lombok&siteid=66',
'https://ntbsatu.com']
rdd = sc.parallelize(seed_urls)
urls_found = 0
max_urls = 100

while urls_found < max_urls:
    links_rdd = rdd.flatMap(get_links).distinct()
    content_rdd = links_rdd.map(get_content)

    temp_rdd = content_rdd.collect()
    urls_found += links_rdd.count()

    # content_rdd.saveAsTextFile("content-" + str(urls_found))
    df = pd.DataFrame(columns=["Teks"])
    df['teks'] = temp_rdd
    totaldata="totaldata"+str(urls_found)
    df.to_csv(totaldata, index=False)
    
    print("data done:",urls_found)

    if urls_found >= max_urls:
        break

    rdd = links_rdd


rdd.saveAsTextFile("urls.txt")
