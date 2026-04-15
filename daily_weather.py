import csv
import requests
from bs4 import BeautifulSoup
import time
import os
import subprocess
from pathlib import Path
from urllib.parse import quote
from urllib.parse import urlparse
import random
BASE_DIR = Path(__file__).resolve().parent
# ==========================================
# 配置部分
# ==========================================
CSV_FILE = 'weather_links.csv'
OUTPUT_URLS_FILE = 'final_2025_weather_urls.txt'
TARGET_YEAR = 2025
ENC_JS_FILE = 'enc.js'

# 请求头 (复用您提供的 headers，防止被反爬)
HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'zh-CN,zh;q=0.9,zh-TW;q=0.8,en;q=0.7',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'referer': 'https://cn.bing.com/',
    'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'cross-site',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36'
}

# monthdata 接口请求头
API_HEADERS = {
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'zh-CN,zh;q=0.9,zh-TW;q=0.8,en;q=0.7',
    'cache-control': 'no-cache',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'origin': 'https://lishi.tianqi.com',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest'
}

def build_crypte(city):
    js_path = BASE_DIR / ENC_JS_FILE
    if not js_path.exists():
        raise FileNotFoundError(f"找不到加密脚本: {js_path}")

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
    node_modules_path = BASE_DIR / 'node_modules'
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
            cwd=str(BASE_DIR)
        )
    except FileNotFoundError as e:
        raise RuntimeError('未检测到 Node.js，请先安装 Node.js。') from e
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or '').strip()
        if "Cannot find module 'crypto-js'" in stderr:
            raise RuntimeError("缺少依赖 crypto-js，请在项目目录执行: npm install crypto-js") from e
        raise RuntimeError(f"调用 enc.js 失败: {stderr}") from e

    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    crypte = lines[-1] if lines else ''
    if not crypte:
        raise RuntimeError('encrypt() 返回为空，请检查 enc.js 是否可正常执行。')
    return crypte


def url_encode_crypte(crypte):
    return quote(crypte, safe='')

def fetch_monthdata(city_code, yyyymm):
    url = f"https://lishi.tianqi.com/monthdata/{city_code}/{yyyymm}"
    referer = f"https://lishi.tianqi.com/{city_code}/{yyyymm}.html"
    headers = dict(API_HEADERS)
    headers['referer'] = referer

    crypte = build_crypte(city_code)
    response = requests.post(
        url,
        headers=headers,
        data={'crypte': crypte},
        timeout=10
    )
    response.raise_for_status()
    return response.text


def fetch_monthdata_and_page(city_code, site_name, yyyymm):

    time.sleep(random.uniform(2, 3))  # 礼貌性延时，避免请求过快被封禁
    base_url = f"https://lishi.{site_name}.com"

    monthdata_url = f"{base_url}/monthdata/{city_code}/{yyyymm}"
    month_page_url = f"{base_url}/{city_code}/{yyyymm}.html"

    crypte_raw = build_crypte(city_code)
    crypte_encoded = url_encode_crypte(crypte_raw)
    payload = "crypte=" + crypte_encoded

    post_headers = dict(API_HEADERS)
    post_headers['referer'] = month_page_url
    monthdata_response = requests.post(
        monthdata_url,
        headers=post_headers,
        data=payload,
        timeout=10
    )
    monthdata_status = monthdata_response.status_code
    monthdata_response.raise_for_status()
    time.sleep(random.uniform(2, 4))  # 礼貌性延时，避免请求过快被封禁
    page_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'zh-CN,zh;q=0.9,zh-TW;q=0.8,en;q=0.7',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'priority': 'u=0, i',
        'referer': f'{base_url}/{city_code}/index.html',
        'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36'
    }
    page_response = requests.get(
        month_page_url,
        headers=page_headers,
        timeout=10
    )
    page_status = page_response.status_code
    page_response.raise_for_status()

    return {
        'crypte_raw': crypte_raw,
        'crypte_encoded': crypte_encoded,
        'monthdata_text': monthdata_response.text,
        'month_page_html': page_response.text,
        'monthdata_status': monthdata_status,
        'page_status': page_status,
    }


def parse_city_and_yyyymm_from_url(url):
    parsed = urlparse(url)
    path_parts = parsed.path.strip('/').split('/')
    if len(path_parts) != 2 or not path_parts[1].endswith('.html'):
        raise ValueError(f"URL 格式不符合预期: {url}")

    city_code = path_parts[0]
    yyyymm = path_parts[1].replace('.html', '')
    if len(yyyymm) != 6 or not yyyymm.isdigit():
        raise ValueError(f"无法从 URL 提取年月: {url}")

    return city_code, yyyymm

# ==========================================
# 步骤 1: 从 CSV 读取并提取纯净的拼音代码
# ==========================================
def extract_pinyin_codes_from_csv(csv_path):
    city_list = []
    if not os.path.exists(csv_path):
        print(f"错误: 找不到文件 {csv_path}")
        return []

    print(f"正在读取 {csv_path} 并提取拼音代码和城市名...")
    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            href_col = None
            name_col = None
            # 优先查找 'href' 列
            if 'href' in fieldnames:
                href_col = 'href'
            else:
                for col in fieldnames:
                    if 'link' in col.lower() or 'url' in col.lower():
                        href_col = col
                        break
                if not href_col:
                    href_col = fieldnames[0]
            # 查找城市名列，优先 'name'，否则找包含 'name' 或 'city' 的列，否则第二列
            if 'name' in fieldnames:
                name_col = 'name'
            else:
                for col in fieldnames:
                    if 'name' in col.lower() or 'city' in col.lower():
                        name_col = col
                        break
                if not name_col and len(fieldnames) > 1:
                    name_col = fieldnames[1]
                elif not name_col:
                    name_col = fieldnames[0]
            print(f"使用列 '{href_col}' 作为链接源, '{name_col}' 作为城市名")
            count = 0
            for row in reader:
                raw_href = row.get(href_col, '').strip()
                city_name = row.get(name_col, '').strip()
                if not raw_href:
                    continue
                clean_path = raw_href.strip('/')
                if clean_path.endswith('index.html'):
                    clean_path = clean_path.replace('index.html', '')
                    clean_path = clean_path.strip('/')
                if '/' in clean_path:
                    pinyin_code = clean_path.split('/')[0]
                else:
                    pinyin_code = clean_path
                if pinyin_code:
                    city_list.append((pinyin_code, city_name))
                    count += 1
            print(f"成功提取 {count} 个拼音代码。示例: {city_list[:5]}")
            return city_list
    except Exception as e:
        print(f"读取 CSV 失败: {e}")
        return []

# ==========================================
# 步骤 2: 生成 URL 列表 (2025年 1-12月)
# ==========================================
def generate_yearly_urls(city_codes, year):
    base_template = "https://lishi.tianqi.com/{code}/{year}{month:02d}.html"
    all_urls = []
    
    print(f"\n开始生成 {year} 年 1-12月 的 URL...")
    
    for code in city_codes:
        for month in range(1, 13):
            url = base_template.format(code=code, year=year, month=month)
            all_urls.append(url)
    
    print(f"总共生成 {len(all_urls)} 个 URL。")
    return all_urls

# ==========================================
# 步骤 3 (可选): 验证性访问 (测试前几个链接)
# ==========================================
def test_access(urls, count=3):
    print(f"\n--- 开始测试访问前 {count} 个链接 (验证有效性) ---")
    for i, url in enumerate(urls):
        if i >= count:
            break
        
        try:
            # 注意：2025年的数据很可能不存在 (404)，这是正常的
            response = requests.get(url, headers=HEADERS, timeout=5)
            status = response.status_code
            msg = "成功" if status == 200 else f"状态码 {status}"
            print(f"[{i+1}] {url} -> {msg}")
        except Exception as e:
            print(f"[{i+1}] {url} -> 请求错误: {e}")
    
    print("--- 测试结束 ---")
    print("提示: 如果大部分返回 404，是因为 2025 年尚未结束，历史天气数据还未生成。")


def get_weather_data(html):
    soup = BeautifulSoup(html, 'lxml')
    weather_list = []
    
    # --- 核心步骤 1: 通过 class 限定 ul 标签 ---
    # find 方法找到第一个符合条件的标签
    target_ul = soup.find('ul', class_='thrui')
    
    if not target_ul:
        print("未找到 class='thrui' 的列表")
        return []

    # --- 核心步骤 2: 在限定的 ul 内部查找所有 li ---
    lis = target_ul.find_all('li')
    
    for li in lis:
        # 数据清洗：排除掉不包含具体天气数据的 li (例如那个"查看更多")
        # 我们检查是否存在 class="th200" 的日期标签，如果没有则跳过
        date_div = li.find('div', class_='th200')
        if not date_div:
            continue
            
        # 获取该 li 下所有的 div 文本
        divs = li.find_all('div')
        
        # 确保数据完整性（防止有些 li 结构不完整导致报错）
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

import json
import pandas as pd


# ==========================================
# 主程序执行
# ==========================================
if __name__ == "__main__":
    # 1. 提取拼音
    city_list = extract_pinyin_codes_from_csv(CSV_FILE)
    if city_list:
        codes = [item[0] for item in city_list]
        code2name = {item[0]: item[1] for item in city_list}
        final_urls = generate_yearly_urls(codes, TARGET_YEAR)
        site_name = 'tianqi'
        all_merged_list = []
        from sys import platform
        def color_text(text, color):
            colors = {'red': '\033[91m', 'green': '\033[92m', 'yellow': '\033[93m', 'reset': '\033[0m'}
            return f"{colors.get(color, '')}{text}{colors['reset']}"

        total = len(final_urls)
        success_count = 0
        fail_count = 0
        def print_progress():
            bar_len = 30
            done = int(bar_len * (success_count + fail_count) / total)
            bar = color_text('█'*done, 'green') + color_text('█'*(bar_len-done), 'red')
            print(f"\r进度: [{bar}] {success_count} 成功, {fail_count} 失败, 共 {total}", end='', flush=True)

        for idx, url in enumerate(final_urls, 1):
            try:
                city_code, yyyymm = parse_city_and_yyyymm_from_url(url)
                city_name = code2name.get(city_code, '')
                result = fetch_monthdata_and_page(city_code, site_name, yyyymm)
                df_monthdata = pd.DataFrame(json.loads(result['monthdata_text']))[['date_str', 'htemp', 'ltemp', 'weather','WD','WS','week']]
                df_weather = pd.DataFrame(get_weather_data(result['month_page_html']))
                if not df_weather.empty:
                    df_weather[['date_str', 'week']] = df_weather['date'].str.extract(r'(\d{4}-\d{2}-\d{2})\s+(.*)')
                    df_weather['ltemp'] = df_weather['max_temp'].str.replace('℃', '', regex=False)
                    df_weather['htemp'] = df_weather['min_temp'].str.replace('℃', '', regex=False)
                    df_weather[['WD', 'WS']] = df_weather['wind'].str.extract(r'([^\s]+)\s*(.*)')
                    df_weather = df_weather[['date_str', 'week', 'ltemp', 'htemp', 'weather', 'WD', 'WS']]
                df_merged = pd.concat([df_monthdata, df_weather], ignore_index=True)
                df_merged['city_code'] = city_code
                df_merged['city_name'] = city_name
                all_merged_list.append(df_merged)
                # 输出状态码
                print(f"\n{city_code} {yyyymm} results: df_merged {len(df_merged)} rows (monthdata: {len(df_monthdata)}, weather: {len(df_weather)})")
                success_count += 1
            except Exception as e:
                fail_count += 1
            print_progress()
            df_all_merged = pd.concat(all_merged_list, ignore_index=True)
            df_all_merged.to_csv("all_merged.csv", index=False, encoding='utf-8-sig')
        print()  # 换行
        # 合并所有df_merged为一张总表
        if all_merged_list:
            df_all_merged = pd.concat(all_merged_list, ignore_index=True)
            df_all_merged.to_csv("all_merged.csv", index=False, encoding='utf-8-sig')
            print("所有数据已合并保存为 all_merged.csv")
    else:
        print("未能提取到城市代码，请检查 CSV 文件格式。")