import datetime

from Model import Model


class TopModel(Model):

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
            sql_to_get_max_price = 'select dateTime, maxPrice from ' + table + \
                                   ' where maxPrice = (select max(maxPrice) from ' + table + ') limit 1'
            maxPriceRow = self.pd.read_sql(sql_to_get_max_price, db)
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
                leftMinPriceRow = self.pd.read_sql(sql_to_get_left_min_price, db)
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
                    rightMinPriceRow = self.pd.read_sql(sql_to_get_right_min_price, db)
                    if rightMinPriceRow.empty:
                        continue
                    else:
                        rightMinPriceDate = rightMinPriceRow['dateTime'][0]
                        rightMinPrice = rightMinPriceRow['minPrice'][0]
                        stockName = rightMinPriceRow['stockName'][0]
                        upRate = int(100 * (maxPrice - leftMinPrice) / leftMinPrice)
                        downRate = int(100 * (maxPrice - rightMinPrice) / maxPrice)
                        data = [code, stockName, leftMinPrice, leftMinPriceDate, maxPrice, maxPriceDate,
                                rightMinPrice, rightMinPriceDate, upRate, downRate, datetime.datetime.now().date()]
                        dataList.append(data)
                        print(str(data))

        df = pd.DataFrame(dataList,
                          columns=['stock_code', 'stock_name',
                                   'left_min_price', 'left_min_price_date',
                                   'max_price', 'max_price_date',
                                   'right_min_price', 'right_min_price_date',
                                   'up_rate', 'down_rate',
                                   'last_modified_date'])
        engine = self.create_engine(
            'mysql+pymysql://' + self.user + ':' + self.password + '@' + self.host + ':' + '3306/' + self.dbname,
            encoding='utf8')

        df.to_sql(
            name='top_model_data',
            con=engine,
            index=False,
            if_exists='append')
        db.close()
