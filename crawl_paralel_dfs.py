import findspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
from bs4 import BeautifulSoup
import pandas as pd
import requests
from time import time
import sys

findspark.init()
findspark.find()

#Buat Spark dan SparkContext dan konfigurasi
conf = SparkConf().set("spark.driver.memory", "8g").set("spark.executor.memory", "8g")
spark = SparkSession.builder \
    .master("local[6]") \
    .appName("WebCrawlSpark") \
    .config(conf=conf) \
    .getOrCreate()

#File input dan dataframe untuk menyimpan hasil data crawling
list_url = 'input.csv'
df = pd.DataFrame(columns=["URL","Teks"])

print("DFS 3")
start_time = time()

#Membaca input seed awal dan mengubah ke dalam bentuk RDD
url_lines = spark.sparkContext.textFile(list_url)
done_crawl=[]

#Fungsi untuk web crawl menggunakan algoritma DFS dan memperhatikan kedalaman crawling
def processRecord(url, depth):
    list_url_baru = []
    if depth >= 0 and len(url) > 0:
        try:
            page = requests.get(url, verify=True, timeout=30)
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
            if len(df) >= 500:
                print("FINAL TOTAL DATA :",len(df),"WAKTU :", str(round(time() - start_time,2)) )
                df.to_csv("DFS2.csv", index=False)
                sys.exit()

            done_crawl.append(url)
            soup = BeautifulSoup(page.text, features="lxml")
            getteks = soup.get_text()
            if "lombok" in getteks.lower():
                teks = getteks.replace("\n"," ").replace("\t"," ")
                df.loc[len(df)] = list([url,teks])

                for link_baru in soup.find_all('a',href=True):
                    if("http" in link_baru['href']):
                        if link_baru.get("href") not in done_crawl:
                            list_url_baru.append(link_baru.get("href"))

                print("Berhasil Crawl :",url,"Total Data :",len(df), "WAKTU :", str(round(time() - start_time,2)))
            else:
                print("Tidak ada keyword di :",url)

    else:
        print("Tidak Ada Link")

    # Melakukan recursive web crawling dengan mengurangi kedalaman sebesar 1
    new_depth = depth - 1
    if new_depth >= 0:
        for new_url in list_url_baru:
            processRecord(new_url, new_depth)

#Proses Crawling
temp = url_lines.flatMap(lambda url: processRecord(url, 3))
temp.count()

df.to_csv("DFS3.csv", index=False)
