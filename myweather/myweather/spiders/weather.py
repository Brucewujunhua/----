from myweather.database.model import MODEL_TABLE
import scrapy
import csv
import os
import json
import subprocess
import random
from pathlib import Path
from urllib.parse import quote
from bs4 import BeautifulSoup
import time
class WeatherSpider(scrapy.Spider):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 启动时加载已采集主键集合
        self.existing_keys = set()
        try:
            model = MODEL_TABLE()

            # 查询所有已采集的city_code, date_str
            rows=model.select('city_code, date_str').query()
            # print(model_MODEL_TABLE.out_query)
            # print(rows)
            for row in rows.out_query:
                # 兼容不同返回格式
                code = row[0]
                date_str = row[1]
                print(f"{code}_{date_str}")
                self.existing_keys.add(f"{code}_{date_str}")
            self.logger.info(f"已加载已采集主键 {len(self.existing_keys)} 条")
        except Exception as e:
            self.logger.warning(f"加载已采集主键失败: {e}")
    name = "weather"
    allowed_domains = ["lishi.tianqi.com"]


    def start_requests(self):
        # 读取城市列表
        base_dir = Path(os.path.dirname(os.path.abspath(__file__))).parent.parent.parent
        csv_path = base_dir / 'weather_links.csv'
        enc_js_path = base_dir / 'enc.js'
        if not csv_path.exists():
            self.logger.error(f"未找到城市csv: {csv_path}")
            return
        city_list = []
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                city_name = row.get('city', '').strip()
                href = row.get('href', '').strip()
                if not city_name or not href:
                    continue
                code = href.strip('/').split('/')[0]
                city_list.append((code, city_name))
        target_year = 2025
        for code, city_name in city_list:
            for month in range(1, 13):
                yyyymm = f"{target_year}{month:02d}"
                # 只爬未采集过的
                # 由于date_str为天级，需在item pipeline里做最终去重
                meta = {
                    'city_code': code,
                    'city_name': city_name,
                    'yyyymm': yyyymm,
                    'enc_js_path': str(enc_js_path),
                    'base_dir': str(base_dir),
                }
                monthdata_url = f"https://lishi.tianqi.com/monthdata/{code}/{yyyymm}"
                page_url = f"https://lishi.tianqi.com/{code}/{yyyymm}.html"
                # 只要该月有一天未采集就采集整月
                month_key = f"{code}_{yyyymm}"
                # 若所有天都已采集则跳过（此处简化，实际可在pipeline里细化）
                if not any(key.startswith(f"{code}_") and key[1+len(code):1+len(code)+8]==yyyymm for key in self.existing_keys):
                    time.sleep(3)
                    yield scrapy.Request(
                        url=monthdata_url,
                        callback=self.parse_monthdata,
                        meta=meta,
                        headers={
                            'referer': page_url,
                            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
                            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                            'x-requested-with': 'XMLHttpRequest',
                        },
                        dont_filter=True,
                        method='POST',
                        body=f"crypte={self.build_crypte(code, enc_js_path, base_dir)}"
                    )

    def build_crypte(self, city, enc_js_path, base_dir):
        js_path = Path(enc_js_path)
        runner_js = (
            "const fs = require('fs');"
            "global.CryptoJS = require('crypto-js');"
            "const city = process.argv[1];"
            "let code = fs.readFileSync(process.argv[2], 'utf8');"
            "code = code.replace(/^\\s*import\\s+CryptoJS\\s+from\\s+['\"]crypto-js['\"];?\\s*/m, '');"
            "code = code.replace(/^\\s*console\\.log\\s*\\(\\s*build_crypte\\([\\s\\S]*?\\)\\s*\\)\\s*;?\\s*$/gm, '');"
            "global.city = city;"
            "eval(code);"
            "const result = typeof build_crypte === 'function' ? build_crypte(city) : encrypt();"
            "process.stdout.write(result);"
        )
        env = os.environ.copy()
        node_modules_path = Path(base_dir) / 'node_modules'
        if node_modules_path.exists():
            env['NODE_PATH'] = str(node_modules_path)
        try:
            result = subprocess.run(
                ['node', '-e', runner_js, city, str(js_path)],
                check=True,
                capture_output=True,
                text=True,
                encoding='utf-8',
                env=env,
                cwd=str(base_dir)
            )
        except Exception as e:
            self.logger.error(f"Node加密失败: {e}")
            return ''
        lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        crypte = lines[-1] if lines else ''
        return quote(crypte, safe='')

    def parse_monthdata(self, response):
        meta = response.meta
        city_code = meta['city_code']
        city_name = meta['city_name']
        yyyymm = meta['yyyymm']
        enc_js_path = meta['enc_js_path']
        base_dir = meta['base_dir']
        monthdata_text = response.text
        print('monthdata_text:',response.text)
        # 继续请求页面HTML
        page_url = f"https://lishi.tianqi.com/{city_code}/{yyyymm}.html"
        meta['monthdata_text'] = monthdata_text
        yield scrapy.Request(
            url=page_url,
            callback=self.parse_month_page,
            meta=meta,
            headers={
                'referer': f'https://lishi.tianqi.com/{city_code}/index.html',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
            },
            dont_filter=True
        )

    def parse_month_page(self, response):
        meta = response.meta
        city_code = meta['city_code']
        city_name = meta['city_name']
        yyyymm = meta['yyyymm']
        monthdata_text = meta['monthdata_text']
        month_page_html = response.text
        # 解析页面天气
        weather_list = self.get_weather_data(month_page_html)
        # 解析 monthdata
        try:
            monthdata = json.loads(monthdata_text)
        except Exception:
            monthdata = []
        # 输出 item
        # print(f"解析完成: {city_name} {yyyymm}，天气条数: {len(weather_list)}，月数据条数: {len(monthdata)}")
        yield {
            'city_code': city_code,
            'city_name': city_name,
            'yyyymm': yyyymm,
            'monthdata': monthdata,
            'weather_list': weather_list,
        }

    def get_weather_data(self, html):
        soup = BeautifulSoup(html, 'lxml')
        weather_list = []
        target_ul = soup.find('ul', class_='thrui')
        if not target_ul:
            return []
        lis = target_ul.find_all('li')
        for li in lis:
            date_div = li.find('div', class_='th200')
            if not date_div:
                continue
            divs = li.find_all('div')
            if len(divs) >= 5:
                data = {
                    "date": divs[0].text.strip(),
                    "max_temp": divs[1].text.strip(),
                    "min_temp": divs[2].text.strip(),
                    "weather": divs[3].text.strip(),
                    "wind": divs[4].text.strip()
                }
                weather_list.append(data)
        return weather_list
