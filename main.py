#!/usr/bin/python
# -*- coding: utf-8 -*-
import datetime

import pandas as pd
import pymysql
from sqlalchemy import create_engine

from BottomModel import BottomModel
from TopModel import TopModel


def run():
    host = 'localhost'
    user = 'root'
    password = 'memore8111'
    dbname = 'STOCK_DATA'
    startTime = datetime.datetime.now()
    print('start time: {}'.format(startTime))
    conn = pymysql.connect(host=host, user=user, password=password, db=dbname)
    engine = create_engine(
        'mysql+pymysql://' + user + ':' + password + '@' + host + ':' + '3306/' + dbname,
        encoding='utf8')
    dateRow = pd.read_sql("select dateTime from S_000001 order by dateTime desc limit 1", conn)
    lastTxnDateInDB = ''
    if dateRow.empty:
        print('db is empty')
        return
    else:
        lastTxnDateInDB = dateRow['dateTime'][0]
        print('Last txn date is {}, today\'s date is {}'.format(lastTxnDateInDB, datetime.datetime.now().date()))
        if lastTxnDateInDB < str(datetime.datetime.now().date()):
            print('probably need to fetch latest data first')
        else:
            print('data is up to date')
    BottomModel(conn, engine).getModel("")
    # lastTxnDateInDB should be last txn date in db, probably need to fetch latest data first!!!!

    TopModel(conn, engine).getModel(lastTxnDateInDB)
    conn.close()
    endTime = datetime.datetime.now()
    print('end time: {}'.format(endTime))
    print('total time: {}'.format(endTime - startTime))


try:
    run()
except Exception as e:
    print(str(e))
