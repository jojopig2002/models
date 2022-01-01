class Model:
    def __init__(self, conn, engine):
        self.engine = engine
        self.conn = conn

    def getModel(self, currentDate):
        pass

    def getStockTableList(self):
        tableList = []
        cursor = self.conn.cursor()
        cursor.execute('show tables where tables_in_stock_data like "s_%"')
        for i in cursor:
            tableList.append(str(i))
        return tableList

    def truncateTable(self, tableName):
        cursor = self.conn.cursor()
        cursor.execute('truncate ' + tableName)
        self.conn.commit()
