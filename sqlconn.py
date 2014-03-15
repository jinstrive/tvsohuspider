import MySQLdb

class DataPersist(object):
    
    def __init__(self):
        self.db = self._getconnect()
    
    def _getconnect(self):
        db = MySQLdb.connect(host='localhost',user='root',passwd='root',db='test',port=3306)
        cur = db.cursor()
        sql_create_table = """CREATE TABLE IF NOT EXIST SOHUTV (
             ID INT AUTO_INCREMENT PRIMARY KEY,
             URL VARCHAR(200),
             TITLE VARCHAR(100),
             DESCRIPTION VARCHAR(200),  
             KEYWORD VARCHAR(200)
             )"""
        cur.execute(sql_create_table)
        return db
    
    def insert(self,mess):
        sql = "INSERT INTO SOHUTV(URL, \
               TITLE, DESCRIPTION, KEYWORD) \
               VALUES ('%s', '%s', '%s', '%s')" % \
               mess['url'],mess['title'],mess['description'],mess['keyword']
        try:
            cursor = self.db.cursor()
            cursor.execute(sql)
            self.db.commit()
            cursor.close()
        except:
            self.db.rollback()


