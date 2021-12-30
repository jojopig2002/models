class Model:
    def __init__(self, host, user, password, dbname, pymysql, re, pd, create_engine):
        self.host = host
        self.user = user
        self.password = password
        self.dbname = dbname
        self.pymysql = pymysql
        self.re = re
        self.pd = pd
        self.create_engine = create_engine

    def getModel(self):
        pass
