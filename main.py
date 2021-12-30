#!/usr/bin/python
# -*- coding: utf-8 -*-
import datetime
import re

import pandas as pd
import pymysql
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

from BottomModel import BottomModel
from TopModel import TopModel


def run():
    host = 'localhost'
    user = 'root'
    password = 'xxx'
    dbname = 'STOCK_DATA'
    print('start time: {}'.format(datetime.datetime.now()))
    # BottomModel(host, user, password, dbname, pymysql, re, pd, create_engine).getModel()
    TopModel(host, user, password, dbname, pymysql, re, pd, create_engine).getModel()
    print('end time: {}'.format(datetime.datetime.now()))


run()
