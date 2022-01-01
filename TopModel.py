import datetime
import re

import pandas as pd

from Model import Model


class TopModel(Model):
    outputTable = 'top_model_data'

    def getModel(self, lastTxnDateInDB):
        dataList = []
        self.truncateTable(self.outputTable)
        tableList = self.getStockTableList()
        for tableName in tableList:
            code = re.sub('\D', '', tableName)
            table = 's_' + code
            sql_to_get_max_price = 'select dateTime, maxPrice from ' + table + \
                                   ' where maxPrice = (select max(maxPrice) from ' + table + ') limit 1'
            maxPriceRow = pd.read_sql(sql_to_get_max_price, self.conn)
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
                leftMinPriceRow = pd.read_sql(sql_to_get_left_min_price, self.conn)
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
                    rightMinPriceRow = pd.read_sql(sql_to_get_right_min_price, self.conn)
                    if rightMinPriceRow.empty:
                        continue
                    else:
                        rightMinPriceDate = rightMinPriceRow['dateTime'][0]
                        rightMinPrice = rightMinPriceRow['minPrice'][0]
                        stockName = rightMinPriceRow['stockName'][0]
                        upRate = int(100 * (maxPrice - leftMinPrice) / leftMinPrice)
                        downRate = int(100 * (maxPrice - rightMinPrice) / maxPrice)
                        sql_to_get_current_date_data = 'select endPrice from ' + table + ' where datetime = "' + \
                                                       lastTxnDateInDB + '"'
                        currentDataRow = pd.read_sql(sql_to_get_current_date_data, self.conn)
                        if currentDataRow.empty:
                            continue
                        else:
                            currentEndPrice = currentDataRow['endPrice'][0]
                            backRate = int(100 * ((currentEndPrice - rightMinPrice) / rightMinPrice))
                            data = [code, stockName, leftMinPrice, leftMinPriceDate, maxPrice, maxPriceDate,
                                    rightMinPrice, rightMinPriceDate, upRate, downRate, backRate,
                                    str(datetime.datetime.now().date()) + ' ' + str(datetime.datetime.now().time())]
                            dataList.append(data)
                            # print(str(data))

        print('total records to update top model is {}'.format(len(dataList)))
        df = pd.DataFrame(dataList,
                          columns=['stock_code', 'stock_name',
                                   'left_min_price', 'left_min_price_date',
                                   'max_price', 'max_price_date',
                                   'right_min_price', 'right_min_price_date',
                                   'up_rate', 'down_rate', 'back_rate',
                                   'last_modified_date'])
        df.to_sql(
            name=self.outputTable,
            con=self.engine,
            index=False,
            if_exists='append')
