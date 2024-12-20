import asyncio
import os
from decimal import Decimal
from datetime import datetime, timedelta
import pandas as pd
import pymysql
import requests
from lxml import html
import json
import csv
import schedule
import time
from time import sleep
from db.tools_db_new_sp import DbNewSpTools
from db.tools_db_sp import DbSpTools


def get_proxies(region):
    proxies = {
        "http": "http://192.168.2.165:7890",
        "https": "https://daili.deepbi.com"
    }
    if region == "JP":
        return proxies
    else:
        return {}


def generate_urls(market, classification_rank_classification_id):
    # URL 模板
    url_templates = {
    "US": [
        "https://www.amazon.com/Best-Sellers-Clothing-Shoes-Jewelry-Mens-Thermal-Underwear-Tops/zgbs/fashion/{}/ref=zg_bs_pg_1_fashion?_encoding=UTF8&pg=1",
        "https://www.amazon.com/Best-Sellers-Clothing-Shoes-Jewelry-Mens-Thermal-Underwear-Tops/zgbs/fashion/{}/ref=zg_bs_pg_2_fashion?_encoding=UTF8&pg=2"
    ],
    "UK": [
        "https://www.amazon.co.uk/Best-Sellers-Fashion-Mens-Thermal-Sets/zgbs/fashion/{}/ref=zg_bs_pg_1_fashion?_encoding=UTF8&pg=1",
        "https://www.amazon.co.uk/Best-Sellers-Fashion-Mens-Thermal-Sets/zgbs/fashion/{}/ref=zg_bs_pg_2_fashion?_encoding=UTF8&pg=2"
    ],
    "JP": [
        "https://www.amazon.co.jp/-/zh/gp/bestsellers/fashion/{}/ref=zg_bs_pg_1_fashion?ie=UTF8&pg=1",
        "https://www.amazon.co.jp/-/zh/gp/bestsellers/fashion/{}/ref=zg_bs_pg_2_fashion?ie=UTF8&pg=2"
    ],
    "DE": [
        "https://www.amazon.de/gp/bestsellers/fashion/{}/ref=zg_bs_pg_1_fashion?ie=UTF8&pg=1",
        "https://www.amazon.de/gp/bestsellers/fashion/{}/ref=zg_bs_pg_2_fashion?ie=UTF8&pg=2"
    ],
    "FR": [
        "https://www.amazon.fr/gp/bestsellers/fashion/{}/ref=zg_bs_pg_1_fashion?ie=UTF8&pg=1",
        "https://www.amazon.fr/gp/bestsellers/fashion/{}/ref=zg_bs_pg_2_fashion?ie=UTF8&pg=2"
    ],
    "IT": [
        "https://www.amazon.it/gp/bestsellers/fashion/{}/ref=zg_bs_pg_1_fashion?ie=UTF8&pg=1",
        "https://www.amazon.it/gp/bestsellers/fashion/{}/ref=zg_bs_pg_2_fashion?ie=UTF8&pg=2"
    ],
    "ES": [
        "https://www.amazon.es/gp/bestsellers/fashion/{}/ref=zg_bs_pg_1_fashion?ie=UTF8&pg=1",
        "https://www.amazon.es/gp/bestsellers/fashion/{}/ref=zg_bs_pg_2_fashion?ie=UTF8&pg=2"
    ],
    "AU": [
        "https://www.amazon.com.au/gp/bestsellers/computers/{}/ref=zg_bs_pg_1_computers?ie=UTF8&pg=1",
        "https://www.amazon.com.au/gp/bestsellers/computers/{}/ref=zg_bs_pg_2_computers?ie=UTF8&pg=2"
    ],
    "CA": [
        "https://www.amazon.ca/Best-Sellers-Clothing-Accessories/zgbs/{}/ref=zg_bs_pg_1_apparel?_encoding=UTF8&pg=1",
        "https://www.amazon.ca/Best-Sellers-Clothing-Accessories/zgbs/{}/ref=zg_bs_pg_2_apparel?_encoding=UTF8&pg=2"
    ],
    "MX": [
        "https://www.amazon.com.mx/gp/bestsellers/{}/ref=zg_bs_pg_1_shoes?ie=UTF8&pg=1",
        "https://www.amazon.com.mx/gp/bestsellers/{}/ref=zg_bs_pg_2_shoes?ie=UTF8&pg=2"
    ]
    }
    if market not in url_templates:
        raise ValueError(f"未知的市场：{market}")
    return [url.format(classification_rank_classification_id) for url in url_templates[market]]


async def pachong(db, brand, market, classification_rank_classification_id):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    urls = generate_urls(market, classification_rank_classification_id)
    all_asin_data = []

    def extract_asin_data(url):
        while True:
            try:
                response = requests.get(url, headers=headers, proxies=get_proxies(market))
                if response.status_code == 200:
                    tree = html.fromstring(response.content)
                    asin_list = []
                    product_elements = tree.xpath('//*[@data-client-recs-list]')
                    for element in product_elements:
                        asin_value = element.attrib.get('data-client-recs-list')
                        if asin_value:
                            asin_list.append(asin_value)
                        else:
                            raise print(f"请求失败，状态码: {response.status_code}")
                    return asin_list
                else:
                    print(f"请求失败，状态码：{response.status_code}")
                    sleep(1)  # 如果请求失败，等待5秒后重试

            except requests.exceptions.RequestException as e:
                # 捕获所有请求相关的异常
                print(f"请求失败，错误信息：{e}")
                sleep(1)  # 等待5秒后重试


    while len(all_asin_data) < 2:
        all_asin_data = []  # 清空之前的数据
        for url in urls:
            asin_data = extract_asin_data(url)
            all_asin_data.extend(asin_data)
        #print(all_asin_data)
        if len(all_asin_data) < 2:
            print(f"当前数据量 {len(all_asin_data)}，重新获取数据...")

    updates = []
    today = datetime.today()
    cur_time = today.strftime('%Y-%m-%d')
    for asin_value in all_asin_data:
        try:
            json_data = json.loads(asin_value)
            for item in json_data:
                product_id = item.get('id')
                rank = item['metadataMap'].get('render.zg.rank')

                updates.append({
                    'market': market,
                    'classification_id': classification_rank_classification_id,
                    'Asin': product_id,
                    'Rank': rank,
                    'Date': cur_time
                })
        except json.JSONDecodeError:
            print("JSON 解码错误")

    api = DbNewSpTools(db, brand, market)
    await api.init()
    await api.batch_expanded_asin_info(updates)
    return f"已抓取分类{classification_rank_classification_id}的top100竞品ASIN"



def expanded_asin(db,brand,market):
    api = DbSpTools(db, brand, market)
    sql_results = asyncio.run(api.get_classification_id(market))
    massage = []
    if sql_results is not None and len(sql_results) > 0:
        for row in sql_results:
            classification_rank_classification_id = row
            # loop = asyncio.get_event_loop()
            # loop.run_until_complete(pachong(db, brand, market, classification_rank_classification_id))
            info = asyncio.run(pachong(db, brand, market, classification_rank_classification_id))
            massage.append(info)
        print(massage)
        return massage
    else:
        return [f"品牌{brand} 国家{market}托管ASIN无小分类，无TOP100竞品ASIN"]

def job():
    brands_and_countries = {
        'amazon_ads': {
            'brand': 'LAPASA',
            'countries': ["US", "FR", "IT", "DE", "ES", "UK", "JP"]
        },
        'amazon_huangjunxi': {
            'brand': 'keimi',
            'countries': ['AU', 'DE', 'UK']
        },
        'amazon_outdoormaster_jp': {
            'brand': 'OutdoorMaster',
            'countries': ["JP"]
        },
        'amazon_bdzx': {
            'brand': 'DELOMO',
            'countries': ['IT', 'ES', 'DE', 'FR']
        },
        'amazon_bdzx_delomo': {
            'brand': 'DELOMO',
            'countries': ['US']
        },
        'amazon_52_ColorNymph': {
            'brand': '5-Star',
            'countries': ['US']
        },
        'amazon_outdoormaster': {
            'brand': 'OutdoorMaster',
            'countries': ['IT', 'ES', 'FR']
        },
        'amazon_huixin': {
            'brand': 'YOURUN',
            'countries': ['US']
        },
        'amazon_60_114_WL': {
            'brand': 'Ethomes',
            'countries': ['US']
        },
        'amazon_61_B3dianpu': {
            'brand': 'M SHUYUN',
            'countries': ['US']
        },
        'amazon_62_241_WL': {
            'brand': 'ARZER',
            'countries': ['US']
        },
        'amazon_63_HJS_US': {
            'brand': 'Altrobene',
            'countries': ['US']
        },
        'amazon_64_ZAPJQL': {
            'brand': 'ZAPJQL',
            'countries': ['JP']
        },
        'amazon_65_SEDY_US': {
            'brand': 'HORUSDY',
            'countries': ['US']
        },
        'amazon_66_236_WL': {
            'brand': 'eeivs',
            'countries': ['US']
        },
        'amazon_67_PioneerCampOutdoor': {
            'brand': 'Pioneer Camp',
            'countries': ['US']
        },
        'amazon_68_LILLEPRINS': {
            'brand': 'LILLEPRINS',
            'countries': ['US']
        },
        'amazon_44_lifeplus': {
            'brand': 'LaCyan',
            'countries': ['US']
        },
        'amazon_70_Tzirci': {
            'brand': 'Tzirci',
            'countries': ['US']
        },
    }
    for key, value in brands_and_countries.items():
        brand = value.get('brand', value['brand'])
        countries = value['countries']
        for country in countries:
            expanded_asin(key, brand, country)

# # Schedule the job to run at 8:00 AM every day
# schedule.every().day.at("08:00").do(job)
#
# # Keep the script running
# while True:
#     schedule.run_pending()
#     time.sleep(60*10)  # Check every minute

if __name__ == "__main__":
    job()

