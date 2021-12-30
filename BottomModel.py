import datetime

from Model import Model


class BottomModel(Model):
    def getModel(self):
        tableList = []
        dataList = []
        db = self.pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.dbname)
        cursor = db.cursor()
        cursor.execute('show tables where tables_in_stock_data like "s_%"')
        for i in cursor:
            tableList.append(str(i))
        for tableName in tableList:
            code = self.re.sub('\D', '', tableName)
            table = 's_' + code
            sql_to_get_min_price = 'select dateTime, minPrice from ' + table + \
                                   ' where minPrice = (select  min(minPrice) from ' + table + ') limit 1'
            minPriceRow = self.pd.read_sql(sql_to_get_min_price, db)
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
                leftMaxPriceRow = self.pd.read_sql(sql_to_get_left_max_price, db)
                if leftMaxPriceRow.empty:
                    continue
                else:
                    leftMaxPriceDate = leftMaxPriceRow['dateTime'][0]
                    leftMaxPrice = leftMaxPriceRow['maxPrice'][0]
                    sql_to_get_right_max_price = "select dateTime, maxPrice, stockName from " + table + \
                                                 " where datetime >= '" + minPriceDate + \
                                                 "' and datetime <= DATE_ADD('" + minPriceDate + \
                                                 "', INTERVAL 12 MONTH) and maxPrice = (select max(maxPrice) from " + \
                                                 table + \
                                                 " where datetime >= '" + minPriceDate + \
                                                 "' and datetime <= DATE_ADD('" + \
                                                 minPriceDate + "', INTERVAL 12 MONTH)) limit 1"
                    rightMaxPriceRow = self.pd.read_sql(sql_to_get_right_max_price, db)
                    if rightMaxPriceRow.empty:
                        continue
                    else:
                        rightMaxPriceDate = rightMaxPriceRow['dateTime'][0]
                        rightMaxPrice = rightMaxPriceRow['maxPrice'][0]
                        stockName = rightMaxPriceRow['stockName'][0]
                        downRate = int(100 * (leftMaxPrice - minPrice) / leftMaxPrice)
                        upRate = int(100 * (rightMaxPrice - minPrice) / minPrice)
                        data = [code, stockName, leftMaxPrice, leftMaxPriceDate, minPrice, minPriceDate,
                                rightMaxPrice, rightMaxPriceDate, downRate, upRate, datetime.datetime.now().date()]
                        dataList.append(data)
                        print(str(data))

        df = self.pd.DataFrame(dataList,
                               columns=['stock_code', 'stock_name',
                                        'left_max_price', 'left_max_price_date',
                                        'min_price', 'min_price_date',
                                        'right_max_price', 'right_max_price_date',
                                        'down_rate', 'up_rate',
                                        'last_modified_date'])
        engine = self.create_engine(
            'mysql+pymysql://' + self.user + ':' + self.password + '@' + self.host + ':' + '3306/' + self.dbname,
            encoding='utf8')

        df.to_sql(
            name='bottom_model_data',
            con=engine,
            index=False,
            if_exists='append')
        db.close()
