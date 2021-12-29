#!/usr/bin/python
# -*- coding: utf-8 -*-
import datetime
import re

import pandas as pd
import pymysql
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine


def getBottomModel(host, user, password, dbname):
    tableList = []
    bottomDataList = []
    db = pymysql.connect(host=host, user=user, password=password, db=dbname)
    cursor = db.cursor()
    cursor.execute('show tables where tables_in_stock like "s_%"')
    for i in cursor:
        tableList.append(str(i))
    for tableName in tableList:
        code = re.sub('\D', '', tableName)
        table = 's_' + code
        sql_to_get_min_price = 'select dateTime, minPrice from ' + table + \
                               ' where minPrice = (select  min(minPrice) from ' + table + ') limit 1'
        minPriceRow = pd.read_sql(sql_to_get_min_price, db)
        if minPriceRow.empty:
            continue
        else:
            minPriceDate = minPriceRow['dateTime'][0]
            minPrice = minPriceRow['minPrice'][0]
            sql_to_get_left_max_price = "select dateTime, maxPrice from " + table + \
                                        " where datetime <= '" + minPriceDate + \
                                        "' and datetime >= DATE_SUB('" + minPriceDate + \
                                        "', INTERVAL 3 MONTH) and maxPrice = (select max(maxPrice) from " + table + \
                                        " where datetime <= '" + minPriceDate + \
                                        "' and datetime >= DATE_SUB('" + \
                                        minPriceDate + "', INTERVAL 3 MONTH)) limit 1"
            leftMaxPriceRow = pd.read_sql(sql_to_get_left_max_price, db)
            if leftMaxPriceRow.empty:
                continue
            else:
                leftMaxPriceDate = leftMaxPriceRow['dateTime'][0]
                leftMaxPrice = leftMaxPriceRow['maxPrice'][0]
                sql_to_get_right_max_price = "select dateTime, maxPrice, stockName from " + table + \
                                             " where datetime >= '" + minPriceDate + \
                                             "' and datetime <= DATE_ADD('" + minPriceDate + \
                                             "', INTERVAL 3 MONTH) and maxPrice = (select max(maxPrice) from " + \
                                             table + \
                                             " where datetime >= '" + minPriceDate + \
                                             "' and datetime <= DATE_ADD('" + \
                                             minPriceDate + "', INTERVAL 3 MONTH)) limit 1"
                rightMaxPriceRow = pd.read_sql(sql_to_get_right_max_price, db)
                if rightMaxPriceRow.empty:
                    continue
                else:
                    rightMaxPriceDate = rightMaxPriceRow['dateTime'][0]
                    rightMaxPrice = rightMaxPriceRow['maxPrice'][0]
                    stockName = rightMaxPriceRow['stockName'][0]
                    downRate = int(100 * (float(leftMaxPrice) - float(minPrice)) / float(leftMaxPrice))
                    upRate = int(100 * (float(rightMaxPrice) - float(minPrice)) / float(minPrice))
                    data = [code, stockName, float(leftMaxPrice), leftMaxPriceDate, float(minPrice), minPriceDate,
                            float(rightMaxPrice), rightMaxPriceDate, downRate, upRate, datetime.datetime.now().date()]
                    bottomDataList.append(data)
                    print(str(data))

    df = pd.DataFrame(bottomDataList,
                      columns=['stock_code', 'stock_name',
                               'left_max_price', 'left_max_price_date',
                               'min_price', 'min_price_date',
                               'right_max_price', 'right_max_price_date',
                               'down_rate', 'up_rate',
                               'last_modified_date'])
    engine = create_engine(
        'mysql+pymysql://' + user + ':' + password + '@' + host + ':' + '3306/' + dbname,
        encoding='utf8')
    df.to_sql(
        name='bottom_model_data',
        con=engine,
        index=False,
        if_exists='append')
    db.close()


def getTopModel(host, user, password, dbname):
    tableList = []
    topDataList = []
    db = pymysql.connect(host=host, user=user, password=password, db=dbname)
    cursor = db.cursor()
    cursor.execute('show tables where tables_in_stock like "s_%"')
    for i in cursor:
        tableList.append(str(i))
    for tableName in tableList:
        code = re.sub('\D', '', tableName)
        table = 's_' + code
        sql_to_get_max_price = 'select dateTime, maxPrice from ' + table + \
                               ' where maxPrice = (select max(maxPrice) from ' + table + ') limit 1'
        maxPriceRow = pd.read_sql(sql_to_get_max_price, db)
        if maxPriceRow.empty:
            continue
        else:
            maxPriceDate = maxPriceRow['dateTime'][0]
            maxPrice = maxPriceRow['maxPrice'][0]
            sql_to_get_left_min_price = "select dateTime, minPrice from " + table + \
                                        " where datetime <= '" + maxPriceDate + \
                                        "' and datetime >= DATE_SUB('" + maxPriceDate + \
                                        "', INTERVAL 1 MONTH) and minPrice = (select min(minPrice) from " + table + \
                                        " where datetime <= '" + maxPriceDate + \
                                        "' and datetime >= DATE_SUB('" + \
                                        maxPriceDate + "', INTERVAL 1 MONTH)) limit 1"
            leftMinPriceRow = pd.read_sql(sql_to_get_left_min_price, db)
            if leftMinPriceRow.empty:
                continue
            else:
                leftMinPriceDate = leftMinPriceRow['dateTime'][0]
                leftMinPrice = leftMinPriceRow['minPrice'][0]
                sql_to_get_right_min_price = "select dateTime, minPrice, stockName from " + table + \
                                             " where datetime >= '" + maxPriceDate + \
                                             "' and datetime <= DATE_ADD('" + maxPriceDate + \
                                             "', INTERVAL 1 MONTH) and minPrice = (select min(minPrice) from " + \
                                             table + \
                                             " where datetime >= '" + maxPriceDate + \
                                             "' and datetime <= DATE_ADD('" + \
                                             maxPriceDate + "', INTERVAL 1 MONTH)) limit 1"
                rightMinPriceRow = pd.read_sql(sql_to_get_right_min_price, db)
                if rightMinPriceRow.empty:
                    continue
                else:
                    rightMinPriceDate = rightMinPriceRow['dateTime'][0]
                    rightMinPrice = rightMinPriceRow['maxPrice'][0]
                    stockName = rightMinPriceRow['stockName'][0]
                    upRate = int(100 * (maxPrice - leftMinPrice) / leftMinPrice)
                    downRate = int(100 * (maxPrice - rightMinPrice) / maxPrice)
                    data = [code, stockName, leftMinPrice, leftMinPriceDate, maxPrice, maxPriceDate,
                            rightMinPrice, rightMinPriceDate, upRate, downRate, datetime.datetime.now().date()]
                    topDataList.append(data)
                    print(str(data))

    df = pd.DataFrame(topDataList,
                      columns=['stock_code', 'stock_name',
                               'left_min_price', 'left_min_price_date',
                               'max_price', 'max_price_date',
                               'right_min_price', 'right_min_price_date',
                               'up_rate', 'down_rate',
                               'last_modified_date'])
    engine = create_engine(
        'mysql+pymysql://' + user + ':' + password + '@' + host + ':' + '3306/' + dbname,
        encoding='utf8')

    df.to_sql(
        name='top_model_data',
        con=engine,
        index=False,
        if_exists='append')
    db.close()


def run():
    host = 'localhost'
    user = 'root'  # 你的用户名
    password = 'xxx'  # 你的密码
    dbname = 'STOCK_DATA'
    # getBottomModel(host, user, password, dbname)
    getTopModel(host, user, password, dbname)


run()
