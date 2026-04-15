import requests
import csv
url = "https://lishi.tianqi.com/"

payload = {}
headers = {
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

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
from bs4 import BeautifulSoup

# 假设 html_content 是您读取到的 DOCTYPE.txt 文件内容
# with open('DOCTYPE.txt', 'r', encoding='utf-8') as f:
#     html_content = f.read()

html_content = response.text # 实际使用时请替换为真实内容

soup = BeautifulSoup(html_content, 'html.parser')

# 查找所有 title 属性中包含"历史天气"的 <a> 标签
links = soup.find_all('a', title=lambda x: x and "历史天气" in x)

results = []
for tag in links:
    href = tag.get('href')
    # 地名通常位于 title 属性中（去掉"历史天气"后缀）或者标签文本中
    title = tag.get('title', '')
    city_name = title.replace('历史天气', '') if title else tag.get_text(strip=True)
    
    results.append({
        'city': city_name,
        'href': href
    })

# 打印结果
for item in results:
    print(f"地名: {item['city']}, 链接: {item['href']}")

# 如果需要导出到 CSV
# import csv
# with open('weather_links.csv', 'w', newline='', encoding='utf-8-sig') as f:
#     writer = csv.DictWriter(f, fieldnames=['city', 'href'])
#     writer.writeheader()
#     writer.writerows(results)

def extract_cities_from_csv(csv_file_path):
    cities = []
    try:
        with open(csv_file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            
            # 自动识别包含城市名称的列 (通常是 'city' 或第一列)
            # 如果您的 CSV 列名不同，请手动指定，例如 fieldnames=['city', 'href']
            fieldnames = reader.fieldnames
            
            # 尝试寻找包含 'city' 关键字的列，如果没有则默认取第一列
            city_column = None
            if 'city' in fieldnames:
                city_column = 'city'
            elif '地名' in fieldnames:
                city_column = '地名'
            else:
                city_column = fieldnames[0] # 默认取第一列
            
            print(f"检测到城市列名为: '{city_column}'")

            for row in reader:
                city_name = row.get(city_column, '').strip()
                
                # 数据清洗：确保提取的是拼音代码 (如 'dongguan')
                # 如果 CSV 里存的是中文 "东莞"，您可能需要额外的映射表，
                # 但根据之前的逻辑，这里存的应该是 href 中提取出的拼音代码。
                if city_name:
                    # 防止 href 完整路径被误存入 city 列，再次清洗
                    if '/' in city_name:
                        city_name = city_name.strip('/').split('/')[0]
                    cities.append(city_name)
                    
        print(f"成功从 CSV 读取 {len(cities)} 个城市代码。示例: {cities[:5]}")
        return cities
        
    except FileNotFoundError:
        print(f"错误：找不到文件 {csv_file_path}")
        return []
    except Exception as e:
        print(f"读取 CSV 时发生错误: {e}")
        return []


# ==========================================
# 第二步：构建2025年1-12月的URL并访问
# ==========================================
def generate_and_fetch_weather_urls(city_codes, year=2025):
    base_url_template = "https://lishi.tianqi.com/{city}/{year}{month:02d}.html"
    
    results = []
    
    for city in city_codes:
        print(f"\n正在处理城市: {city}")
        for month in range(1, 13): # 1 到 12 月
            # 格式化月份为两位数，例如 1 -> 01, 12 -> 12
            month_str = f"{month:02d}"
            url = base_url_template.format(city=city, year=year, month=month)
            
            # --- 模拟访问逻辑 (实际运行时请替换为真实请求) ---
            # print(f"准备访问: {url}")
            
            # 【真实请求代码示例 - 需安装 requests 库】
            # try:
            #     response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
            #     if response.status_code == 200:
            #         print(f"  [成功] {url} (状态码: 200)")
            #         # 在这里解析 response.text 获取具体天气数据
            #     else:
            #         print(f"  [失败] {url} (状态码: {response.status_code}) - 可能该月数据不存在")
            # except Exception as e:
            #     print(f"  [错误] 访问 {url} 时出错: {e}")
            
            # 【演示输出】
            print(f"  -> 生成URL: {url} (注意：2025年数据可能尚未生成)")
            
            # 礼貌性延时，避免请求过快被封禁 (如果是真实爬虫)
            # time.sleep(0.5) 
            
            results.append(url)
            
    return results

# ==========================================
# 主程序入口
# ==========================================
if __name__ == "__main__":
    # 1. 提取城市
    csv_file = 'weather_links.csv'
    
    # 1. 从 CSV 读取城市列表
    city_list = extract_cities_from_csv(csv_file)
    
    if city_list:
        # 2. 生成并访问 2025年 1-12月 的链接
        # 如果需要其他年份，修改 year 参数，例如 year=2024
        all_urls = generate_and_fetch_weather_urls(city_list, year=2025)
        
        print(f"\n处理完成。总共生成了 {len(all_urls)} 个 URL。")
        print("提示：您可以将生成的 URL 保存到文件，或直接在循环中解析网页内容。")
        
        # 可选：将生成的所有 URL 保存到新文件
        with open('generated_2025_urls.txt', 'w', encoding='utf-8') as f:
            for url in all_urls:
                f.write(url + '\n')
        print("所有链接已保存至 generated_2025_urls.txt")
    else:
        print("未提取到任何城市，请检查文件内容。")