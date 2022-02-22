from bs4 import BeautifulSoup, NavigableString, Comment
import multiprocessing
from tqdm import tqdm
from lib.common import getRaw, strToDate, connectDB
from operator import is_not
from functools import partial

sectionName = {'정치': 'politics', '경제': 'economy', '사회': 'society', '생활': 'live', '세계': 'world', 'IT': 'it',
               '오피니언': 'opinion'}
processNo = 10
batch = 2000
host = 'localhost'


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

        writter = newsSoup.find_all("div", {"class":"journalistcard_summary_inner"})

        return True, newsTitle, newsText, summaryText, category, publishTime, editedTime

    except:
        return False,


def getNews(op):
    oid, aid = op
    succ, *args = crawlNews(oid, aid)
    if not succ:
        return
    newsTitle, newsText, summaryText, category, publishTime, editedTime = args
    return {
        'newsId': aid,
        'title': newsTitle,
        'body': newsText,
        'summary': summaryText,
        'category': category,
        'publishTime': publishTime,
        'editedTime': editedTime
    }


if __name__ == '__main__':
    multiprocessing.freeze_support()
    oid = int(input())

    newsDB, categoryDB, __ = connectDB(host)
    metadataCollection = newsDB['metadata']
    try:
        i = metadataCollection.find_one({"oid": oid})['last']
    except:
        i = 1
    while True:
        with multiprocessing.Pool(processes=processNo) as pool:
            newsList = list(filter(partial(is_not, None), tqdm(
                pool.imap_unordered(getNews, [(oid, x) for x in range(i, i + processNo * batch)]),
                total=processNo * batch, desc="Batch %d - %d" % (i, i + processNo * batch - 1))))

        if len(newsList) > 0:
            newsDB[str(oid)].insert_many(newsList)

            categoryDict = dict()

            for j in newsList:
                for c in j['category']:
                    if c not in categoryDict:
                        categoryDict[c] = []
                    categoryDict[c].append({
                        'oid': oid,
                        'aid': j['newsId']
                    })
            for section, data in categoryDict.items():
                categoryDB[section].insert_many(data)

        metadataCollection.delete_one({"oid": oid})
        metadataCollection.insert_one({
            "oid": oid,
            "last": i + processNo * batch
        })
        i += processNo * batch
