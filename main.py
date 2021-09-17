from bs4 import BeautifulSoup
import requests
from pymongo import MongoClient
import feedparser
from datetime import datetime
from time import mktime

client = MongoClient('mongodb://localhost:27017/')
db = client["nseDB"]
market_pulse = db["market_pulse"]
macro_report = db["macro_report"]
market_report = db["market_report"]
sec_report = db["sec_report"]
files = db["files"]

def get_data(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36', "Upgrade-Insecure-Requests": "1","DNT": "1","Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Accept-Language": "en-US,en;q=0.5","Accept-Encoding": "gzip, deflate"}
    try:
        r = requests.get(url, headers=headers)
        return r.text
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        raise SystemExit(e)
    

soup = BeautifulSoup(get_data("https://www.nseindia.com/resources/publications-nse-market-pulse"), "lxml")
all_files = soup.select(".file")
data = []
for file in all_files:
    if not market_pulse.find_one({"_id": file.get("data-entity-uuid")}):
        data.append({
            "_id" : file.get("data-entity-uuid"),
            "title": file.text,
            "pdf_url": file.get("href")
        })
if data:
    market_pulse.insert_many(data)

soup = BeautifulSoup(get_data("https://www.nseindia.com/resources/publications-macro-reports#href-1"), "lxml")
all_files = soup.select("div[class='card h-100'] > div[class='card-body']")
data = []
for file in all_files:
    if file.h6:
        title = file.h6.text
    else:
        title = file.p.text
    pdfs=[]
    foundPdfs = macro_report.find_one({"title": title})
    for pdf in file.select('.file'):
        pdfs.append({
            "_id" : pdf.get("data-entity-uuid"),
            "pdf_url": pdf.get("href")
        })
    if foundPdfs:
        macro_report.update_one(foundPdfs, {"$set" : {"pdf_urls": pdfs}})
    else:
        data.append({
            "pdf_urls": pdfs,
            "title": title
        })
if data:
    macro_report.insert_many(data)

soup = BeautifulSoup(get_data("https://www.nseindia.com/resources/publications-market-reports"), "lxml")
all_files = soup.select_one("div[class='card h-100'] > div[class='card-body']").select("a")
data = []
for file in all_files:
    if not market_report.find_one({"_id": file.get("data-entity-uuid")}):
        data.append({
            "_id" : file.get("data-entity-uuid"),
            "title": file.text.split()[1],
            "pdf_url": file.get("href")
        })
if data:
    market_report.insert_many(data)

rss_feed = feedparser.parse("https://sec.report/Form/S-1.rss").entries
data = []
for feed in rss_feed:
    if not sec_report.find_one({"_id": feed.id}):
        data.append({
            "_id" : feed.id,
            "title": feed.title,
            "links": [link.href for link in feed.links],
            "summary": feed.summary,
            "date": datetime.fromtimestamp(mktime(feed.published_parsed))
        })
if data:
    sec_report.insert_many(data)