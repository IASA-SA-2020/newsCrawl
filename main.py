import json
import pymongo
import requests
import datetime
from bs4 import BeautifulSoup, NavigableString, Comment
import multiprocessing
from tqdm import tqdm

sectionName = {'정치': 'politics', '경제': 'economy', '사회': 'society', '생활': 'live', '세계': 'world', 'IT': 'it',
               '오피니언': 'opinion'}
processNo = 1
batch = 1
host = 'localhost'


def connectDB():
    while True:
        try:
            conn = pymongo.MongoClient(host, 27017)
            newsDB = conn["newsDB"]
            categoryDB = conn["newsCategory"]
            return newsDB, categoryDB
        except:
            pass


def getNewsURL(oid, aid):
    return 'https://news.naver.com/main/read.nhn?oid=%03d&aid=%010d' % (oid, aid), \
           'https://tts.news.naver.com/article/%03d/%010d/summary' % (oid, aid)


def strToDate(dateStr):
    if '오전' in dateStr:
        return datetime.datetime(int(dateStr.split('.')[0]), int(dateStr.split('.')[1]), int(dateStr.split('.')[2]),
                                 int(dateStr.split('.')[3].split(':')[0].split('오전')[1]),
                                 int(dateStr.split('.')[3].split(':')[1]))
    else:
        return datetime.datetime(int(dateStr.split('.')[0]), int(dateStr.split('.')[1]), int(dateStr.split('.')[2]),
                                 int(dateStr.split('.')[3].split(':')[0].split('오후')[1]) + 12,
                                 int(dateStr.split('.')[3].split(':')[1]))


def getRaw(oid, aid):
    while True:
        try:
            newsURL, summaryURL = getNewsURL(oid, aid)
            newsResponse = requests.get(newsURL)
            newsResponseText = newsResponse.text.replace('<br />', '\n').replace('<br>', '\n')
            summaryResponse = requests.get(summaryURL)
        except:
            continue
        try:
            summary = json.loads(summaryResponse.text)
        except:
            summary = None
        if newsResponse.status_code != 200:
            continue
        return True, newsResponseText, summary


def crawlNews(oid, aid):
    try:
        status, newsResponseText, summary = getRaw(oid, aid)
        if not status:
            return False,
        summarySoup = BeautifulSoup(summary['summary'], 'html.parser')
        summaryText = summarySoup.get_text()
        newsText = ""
        newsSoup = BeautifulSoup(newsResponseText, 'html.parser')
        bodyEl = newsSoup.find(id="articleBodyContents")
        for i in bodyEl:
            if type(i) is NavigableString:
                newsText += i
            elif type(i) is Comment:
                pass
            else:
                if i.name == 'br':
                    newsText += '\n'
                if i.get('data-type') == 'ore':
                    newsText += i.get_text()

        newsText = newsText.replace('\n\n', '\n')
        newsText = newsText.replace('\n', ' ')
        newsText = newsText.replace('  ', ' ')
        newsText = newsText.strip()
        newsText = newsText.encode("utf-8").decode('utf-8')

        newsTitle = newsSoup.find(id="articleTitle").get_text().strip()

        category = []
        for i in newsSoup.find_all("em", {"class": "guide_categorization_item"}):
            category.append(sectionName[i.get_text()])

        publishTime = strToDate(newsSoup.find_all("span", {"class": "t11"})[0].get_text())
        if len(newsSoup.find_all("span", {"class": "t11"})) == 2:
            editedTime = strToDate(newsSoup.find_all("span", {"class": "t11"})[1].get_text())
        else:
            editedTime = strToDate(newsSoup.find_all("span", {"class": "t11"})[0].get_text())

        return True, newsTitle, newsText, summaryText, category, publishTime, editedTime

    except:
        return False,


def getNews(newsDB, categoryDB, oid, aid):
    succ, *args = crawlNews(oid, aid)
    if not succ:
        return
    newsTitle, newsText, summaryText, category, publishTime, editedTime = args
    newsCollection = newsDB["%03d" % oid]
    while True:
        try:
            if newsCollection.find_one({"newsId": aid}):
                return
            newsCollection.insert_one({
                'newsId': aid,
                'title': newsTitle,
                'body': newsText,
                'summary': summaryText,
                'category': category,
                'publishTime': publishTime,
                'editedTime': editedTime
            })
            break
        except:
            pass
    for i in category:
        while True:
            try:
                categoryCollection = categoryDB[i]
                categoryCollection.insert_one({
                    'oid': oid,
                    'aid': aid
                })
                break
            except:
                pass


def processOneNews(op):
    oid, i = op
    newsDB, categoryDB = connectDB()
    getNews(newsDB, categoryDB, oid, i)


if __name__ == '__main__':
    multiprocessing.freeze_support()
    oid = int(input())

    newsDB, _ = connectDB()
    metadataCollection = newsDB['metadata']
    try:
        i = metadataCollection.find_one({"oid": oid})['last']
    except:
        i = 1
    while True:
        pool = multiprocessing.Pool(processes=processNo)
        for _ in tqdm(pool.imap_unordered(processOneNews, [(oid, x) for x in range(i, i + processNo * batch)]),
                      total=processNo * batch, desc="Batch %d - %d" % (i, i + processNo * batch - 1)):
            pass
        pool.close()
        pool.join()
        while True:
            try:
                metadataCollection.delete_one({"oid": oid})
                metadataCollection.insert_one({
                    "oid": oid,
                    "last": i + processNo * batch
                })
                break
            except:
                pass
        i += processNo * batch
