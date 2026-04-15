from myweather.utils.mysqlsever import mySqlDB, Model

class BaseModel(Model):
    _conn = mySqlDB(user='sa', passwd='', db='CaiHua')

class MODEL_TABLE(BaseModel):
    _tbl = 'weather_data'