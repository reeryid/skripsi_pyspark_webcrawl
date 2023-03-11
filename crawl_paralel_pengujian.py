import sys
import findspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
from bs4 import BeautifulSoup
import pandas as pd
from time import time
import requests

findspark.init()
findspark.find()

#Buat Spark dan SparkContext dan konfigurasi
conf = SparkConf().set("spark.driver.memory", "2g").set("spark.executor.memory", "2g")
spark = SparkSession.builder \
    .master("local[2]") \
    .appName("WebCrawlSpark") \
    .config(conf=conf) \
    .getOrCreate()

#File input dan dataframe untuk menyiman hasil data crawling
list_url = 'input.csv'
df = pd.DataFrame(columns=["URL","Teks"])

print("Mulai 2 core 2 rame")
start_time = time()

#Membaca input seed awal dan mengubah ke dalam bentuk RDD
url_lines = spark.sparkContext.textFile(list_url)
done_crawl=[]

#Fungsi untuk web crawl konfigurasi untuk Pengujian Stop di 1000 data
def processRecord(url):
    list_url_baru = []   
    if len(url) > 0:
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
                df.to_csv("2core8ram.csv", index=False)
                sys.exit()

            # if (time() - start_time) % 30 <= 5:
            #     print("Data di Update, Total Data :",len(df),"Waktu :", str(round(time() - start_time,2)) )
            #     df.to_csv("4core2ram.csv", index=False)

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

                print("Berhasil Crawl :",url,"Total Data :",len(df),"Waktu :", str(round(time() - start_time,2)))
            else:
                print("Tidak ada keyword di :",url)
                
    else:
        print("Tidak Ada Link")

    return [list_url_baru]

#Proses Crawling
crawlrun = 0

while crawlrun < 5:
    temp = url_lines.map(processRecord)
    temp_rdd = temp.collect()

    seedBaru = []
    # ACCESS LIST URL BARU
    for i in temp_rdd:
        for listBaru in i[0]:
            seedBaru.append(listBaru)
               
    url_lines = spark.sparkContext.parallelize(seedBaru)
    print("Run ke :", crawlrun+1)
    crawlrun+=1

df.to_csv("2core8ram.csv", index=False)
print("Total Waktu: " + str(round(time() - start_time,2)))
  

