# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

from myweather.database.model import *
import pandas as pd
import json
import os
from itemadapter import ItemAdapter

class MyweatherPipeline:
    # MARKET_COMPETITION_TABLE_model = MARKET_COMPETITION_TABLE()
    MODEL_TABLE_model = MODEL_TABLE()
    def __init__(self):
        self.all_merged_list = []

    def process_item(self, item, spider):
        city_code = item.get('city_code', '')
        city_name = item.get('city_name', '')
        yyyymm = item.get('yyyymm', '')
        monthdata = item.get('monthdata', [])
        weather_list = item.get('weather_list', [])
        # 解析 monthdata
        df_monthdata = pd.DataFrame(monthdata)
        if not df_monthdata.empty:
            keep_cols = [c for c in ['date_str', 'htemp', 'ltemp', 'weather','WD','WS','week'] if c in df_monthdata.columns]
            df_monthdata = df_monthdata[keep_cols]
        # 解析 weather_list
        df_weather = pd.DataFrame(weather_list)
        if not df_weather.empty:
            if 'date' in df_weather.columns:
                df_weather[['date_str', 'week']] = df_weather['date'].str.extract(r'(\d{4}-\d{2}-\d{2})\s+(.*)')
            if 'max_temp' in df_weather.columns:
                df_weather['ltemp'] = df_weather['max_temp'].str.replace('℃', '', regex=False)
            if 'min_temp' in df_weather.columns:
                df_weather['htemp'] = df_weather['min_temp'].str.replace('℃', '', regex=False)
            if 'wind' in df_weather.columns:
                df_weather[['WD', 'WS']] = df_weather['wind'].str.extract(r'([^\s]+)\s*(.*)')
            keep_cols = [c for c in ['date_str', 'week', 'ltemp', 'htemp', 'weather', 'WD', 'WS'] if c in df_weather.columns]
            df_weather = df_weather[keep_cols]
        # 合并
        if not df_monthdata.empty or not df_weather.empty:
            df_merged = pd.concat([df_monthdata, df_weather], ignore_index=True)
            df_merged['city_code'] = city_code
            df_merged['city_name'] = city_name
            self.all_merged_list.append(df_merged)
            # 修复：遍历 DataFrame 行，转为 dict 逐条插入
            for _, row in df_merged.iterrows():
                self.MODEL_TABLE_model.add_datas(row.to_dict())

        return item

    # def close_spider(self, spider):
    #     if self.all_merged_list:
    #         df_all_merged = pd.concat(self.all_merged_list, ignore_index=True)
    #         out_path = os.path.join(os.getcwd(), 'all_merged.csv')
    #         df_all_merged.to_csv(out_path, index=False, encoding='utf-8-sig')
    #         print(f"已合并保存所有数据到 {out_path}")
