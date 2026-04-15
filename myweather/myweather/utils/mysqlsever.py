from os import stat
import pymssql
from colorama import  Fore,  init
import pandas as pd
# from OviSpider.utils.logger import Logger
# logger = Logger(__name__)


def addslashes(s):
    if isinstance(type(s), str):
        s = s.encode('utf-8')
    # noinspection PyBroadException
    try:
        d = {'"': '\\"', "'": "\\'", '\0': '\\\0', '\\': '\\\\'}
        return ''.join(d.get(c, c) for c in s)
    except BaseException:
        return s



# 
class mySqlDB(object):
    conn = None
    cursor = None

    def __new__(cls, user, passwd, db):
        cls.connect(user, passwd, db)
        return cls

    def __del__(self):
        self.close()

    @classmethod
    def connect(cls, user, passwd, db):
        cls.conn = pymssql.connect(server='192.168.118.26',user=user,password=passwd,database=db,charset='utf8')
        cls.cursor = cls.conn.cursor()
        # cls.cursor.execute('SET NAMES utf8mb4')
    

    @classmethod
    def close(cls):
        cls.cursor.close()
        cls.conn.close()

    @classmethod
    def execute(cls, sql, is_dict=0):
        # logger.info('SQL=' + sql)
        if is_dict:
            cls.cursor = cls.conn.cursor(as_dict=True)
        cls.cursor.execute(sql)
        return cls

    @classmethod
    def executemany(cls, sql, value, is_dict=0):
        # logger.info('SQL=' + sql)
        if is_dict:
            cls.cursor = cls.conn.cursor(as_dict=True)
        cls.cursor.executemany(sql, value)
        return cls

    @classmethod
    def query(cls,sql,state='all'):
        cls.cursor.execute(sql)
        if state=='all':
            rows=cls.cursor.fetchall()
            outputrows = []
            try:
                if rows and isinstance(rows,(list,tuple)):#
                    for data_rows in rows:
                        each_rows = ()
                        for data_row in data_rows:
                            if type(data_row)==str:
                                each_rows =each_rows +(data_row.encode('latin1').decode('gbk'),)
                            else: 
                                each_rows = each_rows+(data_row,)
                        outputrows.append(each_rows)
                return outputrows
            except:
                print(Fore.YELLOW + '------>查询无数据<-------' + Fore.RESET)
                return []
        else:
            row=cls.cursor.fetchone()
            return row

    @classmethod
    def commit(cls):
        cls.conn.commit()

    @classmethod
    def execute_commit(cls, sql, is_dict=0):
        cls.execute(sql, is_dict).commit()

    @classmethod
    def executemany_commit(cls, sql, values, is_dict=0):
        cls.executemany(sql, values, is_dict).commit()

class Model(object):
    __SELECT = 'SELECT %s FROM %s'
    __REPLACE = 'REPLACE INTO %s (%s) VALUES (%s)'
    __INSERT = 'INSERT INTO %s (%s) VALUES (%s)'
    __DELETE = 'DELETE FROM %s'
    __WHERE = '%s WHERE %s'
    __ORDER_BY = '%s ORDER BY %s'
    __LIMIT = '%s LIMIT %s'
    __COUNT = 'COUNT(%s)'

    _conn = None
    _tbl = None

    __sql = ''

    def __init__(self):
        self.out_query=[]
        pass

    def select(self, select_str):
        if select_str.find(',') == -1:
            select_str = select_str
        else:
            fields = list()
            for f in select_str.split(','):
                if f.find('as') > 0:
                    p = f.split(' as ')
                    fields.append(p[0].strip() + ' as ' + p[1].strip())
                else:
                    fields.append(f.strip())
                select_str = ','.join(fields)

        self.__sql = self.__SELECT % (select_str, self._tbl)
        # self.out_query=self.__query(self.__SELECT % (select_str, self._tbl))
        
        return self

    def where(self, string):
        self.__sql = self.__WHERE % (self.__sql, string)
        return self

    def order_by(self, string):
        self.__sql = self.__ORDER_BY % (self.__sql, string)
        return self

    def limit(self, num):
        self.__sql = self.__LIMIT % (self.__sql, str(num))
        return self

    def count(self, field_name='*'):
        self.select(self.__COUNT % field_name)
        return self

    def fetchall(self, is_dict=0):
        return self.__execute(self.__sql, is_dict).cursor.fetchall()

    def fetchone(self):
        return self.__execute(self.__sql).cursor.fetchone()

    def replace(self, fields, values):
        self.__execute_commit(self.__REPLACE % (self._tbl, ','.join(fields), ','.join(values)))

    def insert(self, fields, values):
        self.__execute_commit(self.__INSERT % (self._tbl, ','.join(fields), ','.join(values)))

    def add_datas(self, data, replace=False):
        fields = [str(field) for field in data.keys()]
        values = [self.__convert_value(value) for value in data.values()]

        self.replace(fields, values) if replace else self.insert(fields, values)

    def replacemany(self, fields, values):
        tmp_values = ['%s'] * len(fields)
        self.__executemany_commit(self.__REPLACE % (self._tbl, ','.join(fields), ','.join(tmp_values)), values)

    def insertmany(self, fields, values):
        tmp_values = ['%s'] * len(fields)
        self.__executemany_commit(self.__INSERT % (self._tbl, ','.join(fields), ','.join(tmp_values)), values)

    def addmany_datas(self, fields, values, replace=False):
        self.replacemany(fields, values) if replace else self.insertmany(fields, values)

    def update(self, where, **data):
        pass

    def delete(self, where=None):
        sql_delete = self.__DELETE % self._tbl
        sql = sql_delete if where is None else self.__WHERE % (sql_delete, where)
        self.__execute_commit(sql)

    def query(self):
        self.out_query=self.__query(self.__sql)
        return self

    def __execute(self, sql, is_dict=0):
        self._conn.execute(sql, is_dict)
        return self._conn

    def __execute_commit(self, sql, is_dict=0):
        self._conn.execute_commit(sql, is_dict)

    def __executemany(self, sql, is_dict=0):
        self._conn.executemany(sql, is_dict)
        return self._conn

    def __executemany_commit(self, sql, is_dict=0):
        self._conn.executemany_commit(sql, is_dict)

    def __query(self,sql,state='all'):
        return self._conn.query(sql,state)

    @classmethod
    def __convert_value(cls, value):
        return '\'%s\'' % addslashes(value)

if __name__=='__main__':

    class BaseModel(Model):
        _conn = mySqlDB(user='sa', passwd='', db='CaiHua')

    class MODEL_TABLE(BaseModel):
        _tbl = 'weather_data'

    model_MODEL_TABLE=MODEL_TABLE()

    # r=model_MODEL_TABLE.select('*').where("channeltype='悦合集'").query()

    # print(pd.DataFrame(r.out_query))
    model = MODEL_TABLE()
    # 查询所有已采集的city_code, date_str
    rows=model_MODEL_TABLE.select('city_code, date_str').query()
    # print(model_MODEL_TABLE.out_query)
    # print(rows)
    for row in rows.out_query:
        # 兼容不同返回格式
        code = row[0]
        date_str = row[1]
        print(f"{code}_{date_str}")
    # for row in rows:
    #     # 兼容不同返回格式
    #     code = row[0]
    #     date_str = row[1]
    #     print(code,date_str)
    #     # self.existing_keys.add(f"{code}_{date_str}")
