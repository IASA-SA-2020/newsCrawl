import pymongo
import requests
import datetime
import json
import sys
import time

sectionName = {'정치': 'politics', '경제': 'economy', '사회': 'society', '생활': 'live', '세계': 'world', 'IT': 'it',
               '오피니언': 'opinion'}
_oidList = [1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 13, 14, 15, 16, 18, 19, 20, 21, 22, 23, 24, 25, 28, 32, 47, 52, 55, 56, 57,
           81, 88, 91, 96, 227, 437, 448]
oidList=[13, 18]


def connectDB(host):
    while True:
        try:
            conn = pymongo.MongoClient(host, 27017)
            newsDB = conn["newsDB"]
            categoryDB = conn["newsCategory"]
            newsRawDB = conn["newsRawDB"]
            return newsDB, categoryDB, newsRawDB
        except:
            pass


def getNewsURL(oid, aid):
    return 'https://news.naver.com/main/read.nhn?oid=%03d&aid=%010d' % (oid, aid), \
           'https://tts.news.naver.com/article/%03d/%010d/summary' % (oid, aid)


def getRaw(oid, aid):
    while True:
        try:
            newsURL, summaryURL = getNewsURL(oid, aid)
            headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}
            newsResponse = requests.get(newsURL, headers = headers)
            newsResponseText = newsResponse.text.replace('<br />', '\n').replace('<br>', '\n')
            summaryResponse = requests.get(summaryURL, headers = headers)
        except:
            continue
        try:
            summary = json.loads(summaryResponse.text)
        except:
            summary = None
        if newsResponse.status_code != 200:
            continue
        return True, newsResponseText, summary


def strToDate(dateStr):
    if '오전' in dateStr:
        return datetime.datetime(int(dateStr.split('.')[0]), int(dateStr.split('.')[1]), int(dateStr.split('.')[2]),
                                 int(dateStr.split('.')[3].split(':')[0].split('오전')[1]),
                                 int(dateStr.split('.')[3].split(':')[1]))
    else:
        return datetime.datetime(int(dateStr.split('.')[0]), int(dateStr.split('.')[1]), int(dateStr.split('.')[2]),
                                 int(dateStr.split('.')[3].split(':')[0].split('오후')[1]) + 12,
                                 int(dateStr.split('.')[3].split(':')[1]))


def log(str, startTime, processNo, parsedNo):
    sys.stdout.write("\b" * 500)
    print('[%s] %s                                         ' % (datetime.datetime.now(), str))
    sys.stdout.write("\b" * 500)
    e = int(time.time() - startTime)
    print('Running for %02d:%02d:%02d with %d processes. Parsed %d documents until now.' % (
        e // 3600, (e % 3600 // 60), e % 60, processNo, parsedNo), end='', flush=True)


def logGet(str, startTime, processNo, parsedNo):
    sys.stdout.write("\b" * 500)
    print('[%s] %s                                         ' % (datetime.datetime.now(), str))
    sys.stdout.write("\b" * 500)
    e = int(time.time() - startTime)
    print('Running for %02d:%02d:%02d with %d processes. Crawled %d documents until now.' % (
        e // 3600, (e % 3600 // 60), e % 60, processNo, parsedNo), end='', flush=True)
