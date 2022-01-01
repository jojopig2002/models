#!/usr/bin/python
# -*- coding: utf-8 -*-
import datetime

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
    BottomModel(conn, engine).getModel("")
    TopModel(conn, engine).getModel("2021-12-31")
    conn.close()
    endTime = datetime.datetime.now()
    print('end time: {}'.format(endTime))
    print('total time: {}'.format(endTime - startTime))


try:
    run()
except Exception as e:
    print(str(e))
