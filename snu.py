from bs4 import BeautifulSoup
from tqdm import tqdm
from lib.common import getSNURaw, connectDB
import demjson
import numpy as np

host = 'mongodb://localhost'


def crawlNews(page):
    status, html = getSNURaw(page)
    news = []
    if not status:
        return False,
    newsText = ""
    newsSoup = BeautifulSoup(html, 'html.parser')
    els = newsSoup.select('div.fcItem_top.clearfix')
    for i in els:
        try:
            body = i.select('a')[0].text
            factJson = demjson.decode(i.select('script')[0].text.strip()[14:-2].strip())
            score = np.mean(list(factJson['score'].values()))
            if score > 0:
                news.append({'body': body, 'score': score})
        except:
            pass

    return news


newsDB, *_ = connectDB(host)
page = 1
tot = 0
while True:
    li = crawlNews(page)
    if len(li) > 0:
        newsDB['snu'].insert_many(li)
    tot += len(li)
    print('Pushed %d objects' % tot)
    page += 1
