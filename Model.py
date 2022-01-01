import pymysql


class Model:
    def __init__(self, conn, engine):
        self.engine = engine
        self.conn = conn

    def getModel(self, currentDate):
        pass

    def truncateTable(self, tableName):
        cursor = self.conn.cursor()
        cursor.execute()
        cursor.close()
