import requests
from bs4 import BeautifulSoup
from pyspark import SparkContext
import pandas as pd

# fungsi untuk mengambil url dan teks dari website
def get_links_and_text(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    links = []
    text = soup.get_text()
    for link in soup.find_all('a'):
        links.append(link.get('href'))
    return links, text

# membuat SparkContext
sc = SparkContext('local', 'WebCrawler')

# membuat RDD dengan url awal
listurl = [
'https://radarlombok.co.id/',
'https://ntbsatu.com',
'https://lombokpost.jawapos.com/',
'https://www.detik.com/search/searchall?query=lombok&siteid=66',
'https://www.kompas.com/tag/lombok'
]

seed_rdd = sc.parallelize(listurl)

# melakukan web crawling dengan Spark
data_count = 0  # inisialisasi counter data yang sudah diambil
for i in range(5):
    links_rdd = seed_rdd.flatMap(get_links_and_text)
    seed_rdd = links_rdd.flatMap(lambda x: x[0]).distinct()
    
    # menambahkan pengecekan jumlah data yang sudah diambil
    data_count += links_rdd.count()
    if data_count >= 500:
        break

# memasukkan hasil crawling ke dalam DataFrame Pandas
data = {'link': [], 'text': []}
for link, text in links_rdd.collect():
    data['link'].append(link)
    data['text'].append(text)
df = pd.DataFrame(data)

# mencetak hasil
print(df)
