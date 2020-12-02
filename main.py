from bs4 import BeautifulSoup, NavigableString, Comment
import multiprocessing
from tqdm import tqdm
from lib.common import getRaw, strToDate, connectDB

sectionName = {'정치': 'politics', '경제': 'economy', '사회': 'society', '생활': 'live', '세계': 'world', 'IT': 'it',
               '오피니언': 'opinion'}
processNo = 2
batch = 10
host = 'mongodb://user:iasa2020!@localhost'


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
    newsDB, categoryDB, _ = connectDB(host)
    getNews(newsDB, categoryDB, oid, i)


if __name__ == '__main__':
    multiprocessing.freeze_support()
    oid = int(input())

    newsDB, _, __ = connectDB(host)
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
