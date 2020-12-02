import multiprocessing
from lib.common import connectDB, getRaw, log, oidList
import sys
import time

host = 'mongodb://user:iasa2020!@localhost'
chunk = 100
maxProcessNo = 16


def crawlNews(oid, processNo, pushedNo, startTime):
    while True:
        try:
            _, _, newsRawDB = connectDB(host)
            metadataCollection = newsRawDB['metadata']
            try:
                startNo = metadataCollection.find_one({"oid": oid})['last']
            except:
                startNo = 1
            tmpDB = []
            cnt = 0
            pushedNo.value += startNo - 1
            log('Process oid=%03d started at aid=%d' % (oid, startNo), startTime, processNo,
                pushedNo.value)
            for i in range(startNo, 999999999):
                status, newsResponseText, summary = getRaw(oid, i)
                if not status:
                    continue
                tmpDB.append({
                    'body': newsResponseText,
                    'summary': summary,
                    'aid': i
                })
                cnt += 1
                if cnt >= chunk:
                    if len(tmpDB) > 0:
                        newsRawDB[str(oid)].insert_many(tmpDB)
                        pushedNo.value += len(tmpDB)
                    log('Pushed %03d objects to DB at oid=%03d for aid=%d' % (len(tmpDB), oid, i), startTime, processNo,
                        pushedNo.value)
                    tmpDB = []
                    cnt = 0
                    try:
                        metadataCollection.delete_one({"oid": oid})
                        metadataCollection.insert_one({
                            "oid": oid,
                            "last": i
                        })
                    except:
                        pass
        except:
            pass


if __name__ == '__main__':
    multiprocessing.freeze_support()
    print('Crawler main process started.')
    thrs = []
    cnt = 0
    processNo = len(oidList)
    pushedNo = multiprocessing.Value('i', 0)
    startTime = time.time()
    for i in oidList:
        thr = multiprocessing.Process(target=crawlNews, args=(i, processNo, pushedNo, startTime))
        thrs.append(thr)
        thr.start()
        cnt += 1
    for i in thrs:
        i.join()
