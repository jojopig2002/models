import datetime
import re

import pandas as pd

from Model import Model


class BottomModel(Model):
    outputTable = 'bottom_model_data'

    def getModel(self, currentDate):
        dataList = []
        self.truncateTable(self.outputTable)
        tableList = self.getStockTableList()
        for tableName in tableList:
            code = re.sub('\D', '', tableName)
            table = 's_' + code
            sql_to_get_min_price = 'select dateTime, minPrice from ' + table + \
                                   ' where minPrice = (select  min(minPrice) from ' + table + ') limit 1'
            minPriceRow = pd.read_sql(sql_to_get_min_price, self.conn)
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
                leftMaxPriceRow = pd.read_sql(sql_to_get_left_max_price, self.conn)
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
                    rightMaxPriceRow = pd.read_sql(sql_to_get_right_max_price, self.conn)
                    if rightMaxPriceRow.empty:
                        continue
                    else:
                        rightMaxPriceDate = rightMaxPriceRow['dateTime'][0]
                        rightMaxPrice = rightMaxPriceRow['maxPrice'][0]
                        stockName = rightMaxPriceRow['stockName'][0]
                        downRate = int(100 * (leftMaxPrice - minPrice) / leftMaxPrice)
                        upRate = int(100 * (rightMaxPrice - minPrice) / minPrice)
                        data = [code, stockName, leftMaxPrice, leftMaxPriceDate, minPrice, minPriceDate,
                                rightMaxPrice, rightMaxPriceDate, downRate, upRate, str(datetime.datetime.now().date())
                                + ' ' + str(datetime.datetime.now().time())]
                        dataList.append(data)
                        print(str(data))

        df = pd.DataFrame(dataList,
                          columns=['stock_code', 'stock_name',
                                   'left_max_price', 'left_max_price_date',
                                   'min_price', 'min_price_date',
                                   'right_max_price', 'right_max_price_date',
                                   'down_rate', 'up_rate',
                                   'last_modified_date'])
        df.to_sql(
            name=self.outputTable,
            con=self.engine,
            index=False,
            if_exists='append')
