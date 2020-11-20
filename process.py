import multiprocessing
from bs4 import BeautifulSoup, NavigableString, Comment
from lib.common import connectDB, oidList, log, strToDate, sectionName
import sys
import time

host = 'localhost'
chunk = 500
maxProcessNo = 16


def parseNews(oid, processNo, parsedNo, startTime):
    while 1:
        try:
            log('Process oid=%03d started.' % oid, 0, 0, 0)
            newsDB, categoryDB, newsRawDB = connectDB(host)
            while 1:
                li = list(newsRawDB[str(oid)].find().limit(chunk))
                if len(li) == 0:
                    return
                log('Got %d Data from DB at oid=%03d' % (len(li), oid), startTime, processNo,
                    parsedNo.value)
                removeLi = []
                processedNews = []
                categoryDict = dict()
                for news in li:
                    try:
                        removeLi.append({'_id': news['_id']})
                        aid, body, summary = news['aid'], news['body'], news['summary']
                        summarySoup = BeautifulSoup(summary['summary'], 'html.parser')
                        summaryText = summarySoup.get_text()
                        newsText = ""
                        newsSoup = BeautifulSoup(body, 'html.parser')
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
                            if sectionName[i.get_text()] not in categoryDict:
                                categoryDict[sectionName[i.get_text()]] = []
                            categoryDict[sectionName[i.get_text()]].append({
                                'oid': oid,
                                'aid': aid
                            })

                        publishTime = strToDate(newsSoup.find_all("span", {"class": "t11"})[0].get_text())
                        if len(newsSoup.find_all("span", {"class": "t11"})) == 2:
                            editedTime = strToDate(newsSoup.find_all("span", {"class": "t11"})[1].get_text())
                        else:
                            editedTime = strToDate(newsSoup.find_all("span", {"class": "t11"})[0].get_text())

                        processedNews.append({
                            'newsId': aid,
                            'title': newsTitle,
                            'body': newsText,
                            'summary': summaryText,
                            'category': category,
                            'publishTime': publishTime,
                            'editedTime': editedTime
                        })
                    except:
                        pass
                for section, data in categoryDict.items():
                    categoryDB[section].insert_many(data)
                if len(processedNews) > 0:
                    newsDB[str(oid)].insert_many(processedNews)
                    parsedNo.value += len(processedNews)
                log('Parsed %03d objects in DB at oid=%03d' % (len(processedNews), oid), startTime, processNo,
                    parsedNo.value)
                for remove in removeLi:
                    newsRawDB[str(oid)].delete_one(remove)
                log('Dropped %03d objects in RAW at oid=%03d' % (chunk, oid), startTime, processNo,
                    parsedNo.value)
        except:
            pass


if __name__ == '__main__':
    multiprocessing.freeze_support()
    log('Parser main process started.', time.time(), 0, 0)
    thrs = []
    cnt = 0
    processNo = min(maxProcessNo, len(oidList))
    parsedNo = multiprocessing.Value('i', 0)
    startTime = time.time()
    for i in oidList:
        if cnt >= processNo:
            break
        thr = multiprocessing.Process(target=parseNews, args=(i, processNo, parsedNo, startTime))
        thrs.append(thr)
        thr.start()
        cnt += 1
    for i in thrs:
        i.join()
