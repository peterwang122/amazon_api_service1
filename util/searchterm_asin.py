import asyncio
import os
from decimal import Decimal
from datetime import datetime, timedelta
import pandas as pd
import pymysql
import requests
import random
from lxml import html
import json
import csv
import schedule
import time
from time import sleep
from requests_html import AsyncHTMLSession
from db.tools_db_new_sp import DbNewSpTools
from db.tools_db_sp import DbSpTools
from util.proxies import ProxyManager


def get_proxies(region):
    proxies = {
        "http": "http://192.168.2.165:7890",
        "https": "http://192.168.2.165:7890"
    }
    if region in ("JP"):
        return proxies
    else:
        return {}


def generate_urls(market):
    # URL 模板
    url_templates = {
    "UK": "https://www.amazon.co.uk/s?k=",
    "US": "https://www.amazon.com/s?k=",
    "DE": "https://www.amazon.de/s?k=",
    "FR": "https://www.amazon.fr/s?k=",
    "IT": "https://www.amazon.it/s?k=",
    "ES": "https://www.amazon.es/s?k=",
    "JP": "https://www.amazon.co.jp/s?k=",
    "AU": "https://www.amazon.com.au/s?k="
}
    base_url = url_templates.get(market.upper())
    if not base_url:
        print(f"不支持该国家的 Amazon 网站：{market}")
        raise f"不支持该国家的 Amazon 网站：{market}"
    return base_url


async def pachong(db, brand, market, search_term, cache):
    # headers = {
    #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    # }
    # headers = {
    #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    #     'Accept-Language': 'en-US,en;q=0.9',
    #     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    #     'Connection': 'keep-alive',
    #     'Upgrade-Insecure-Requests': '1',
    #     'Cache-Control': 'max-age=0'
    # }
    urls = generate_urls(market)
    all_asin_data = []
    # 处理搜索词，若有空格则替换为 "+"
    search_term = search_term.replace(" ", "+")
    cache_key = (market, search_term)
    if cache_key in cache:
        print(f"已缓存 {market} - {search_term} 的 ASIN，跳过爬取")
        return cache[cache_key]
    async def extract_asin_data(url,session,proxy_manager):
        while True:
            try:
                print(url)
                response = await session.get(url,proxies=proxy_manager.get_proxies(region='JP'))
                if response.status_code == 200 or response.status_code == 503:
                    # 执行 JavaScript
                    await response.html.arender(sleep=6)
                    # 使用 CSS 选择器查询所有包含 data-asin 属性的元素
                    asin_elements = response.html.find('[data-asin]')

                    # 提取所有元素的 data-asin 属性值
                    asins = [element.attrs['data-asin'] for element in asin_elements]
                    asin_list = []
                    for element in asins:
                        if element and element.startswith('B0'):
                            asin_list.append(element)
                    print(asin_list)
                    if len(asin_list) > 0:
                        return asin_list
                    else:
                        await asyncio.sleep(random.uniform(3, 5))  # 如果请求失败，等待5秒后重试
                        return []
                else:
                    print(f"请求失败，状态码：{response.status_code}")
                    await asyncio.sleep(random.uniform(3, 5))  # 如果请求失败，等待5秒后重试
                    return []

            except requests.exceptions.RequestException as e:
                # 捕获所有请求相关的异常
                print(f"请求失败，错误信息：{e}")
                await asyncio.sleep(random.uniform(3, 5))  # 等待5秒后重试
                return []

    for page_num in range(1, 8):
        consecutive_empty_count = 0
        url = f"{urls}{search_term}&page={page_num}&ref=sr_pg_{page_num}"
        print(f"正在处理 {market} - {search_term} 的第 {page_num} 页...")
        asin_data = []
        proxy_manager = ProxyManager()
        while len(asin_data) < 1:
            asin_data = []  # 清空之前的数据
            session = AsyncHTMLSession()
            try:
                asin_data = await extract_asin_data(url, session, proxy_manager)
            except Exception as e:
                print(f"爬取失败，错误：{e}")
            finally:
                await session.close()  # 确保会话关闭
            if len(asin_data) < 1:
                consecutive_empty_count += 1
                print(f"连续返回空数据 {consecutive_empty_count} 次")

                if consecutive_empty_count >= 3:
                    print("连续3次返回空数据，等待10分钟后继续...")
                    await asyncio.sleep(10 * 60)  # 等待10分钟
                    consecutive_empty_count = 0  # 重置计数器
                else:
                    print(f"当前数据量 {len(asin_data)}，重新获取数据...")
        all_asin_data.extend(asin_data)


    cache[cache_key] = all_asin_data
    return all_asin_data




async def searchterm_asin(db,brand,market,day,order):
    api = DbSpTools(db, brand, market)
    sql_results = await api.get_classification_title(market)
    asin_cache = {}
    if sql_results is not None and not sql_results.empty:
        result = sql_results.groupby(['parent_asins', 'market'])[
            'classification_rank_title'].agg(list)
        massage = []
        # 循环处理每一行数据
        for index, ((parent_asin, market), classification_rank_titles) in enumerate(result.items()):
            all_asin_data = []
            seen_asins = set()
            if classification_rank_titles or classification_rank_titles == '':
                print("test:",classification_rank_titles)
                for i in classification_rank_titles:
                    if i:
                        title = i
                        # 解决 asyncio.run() 的问题：改用 get_event_loop()
                        # loop = asyncio.get_event_loop()
                        # try:
                        #     asin_data =loop.run_until_complete(pachong(db, brand, market, title,asin_cache))
                        # finally:
                        #     loop.close()
                        asin_data = await pachong(db, brand, market, title,asin_cache)
                        for asin in asin_data:
                            if asin not in seen_asins:
                                all_asin_data.append(asin)  # 添加到最终的列表
                                seen_asins.add(asin)  # 将该 ASIN 标记为已处理
                                if len(all_asin_data) >= 400:
                                    print("已收集到 400 条 ASIN 数据，停止添加。")
                                    break
                        if len(all_asin_data) >= 400:
                            print("已收集到 1000 条 ASIN 数据，停止添加。")
                            break
            if len(all_asin_data) < 400:
                sercahterms = await api.get_serachterm(market,parent_asin,day,order)
                for sercahterm in sercahterms:
                    if sercahterm:
                        # loop = asyncio.get_event_loop()
                        # try:
                        #     asin_data1 = loop.run_until_complete(pachong(db, brand, market, sercahterm, asin_cache))
                        # finally:
                        #     loop.close()
                        asin_data1 = await pachong(db, brand, market, sercahterm, asin_cache)
                        for asin in asin_data1:
                            if asin not in seen_asins:
                                all_asin_data.append(asin)  # 添加到最终的列表
                                seen_asins.add(asin)  # 将该 ASIN 标记为已处理
                                if len(all_asin_data) >= 400:
                                    print("已收集到 400 条 ASIN 数据，停止添加。")
                                    break
                    if len(all_asin_data) >= 400:
                        break  # 如果外层循环也达到了 1000 条，直接跳出
            print(all_asin_data)
            updates = []
            today = datetime.today()
            cur_time = today.strftime('%Y-%m-%d')
            for asin in all_asin_data:
                try:
                    updates.append({
                        'market': market,
                        'classification_id': parent_asin,
                        'Asin': asin,
                        'Rank': 0,
                        'Date': cur_time
                    })
                except json.JSONDecodeError:
                    print("JSON 解码错误")
            api1 = DbNewSpTools(db, brand, market)
            await api1.init()
            await api1.batch_expanded_asin_info(updates)
            info = f"已抓取父ASIN：{parent_asin}的搜索词竞品ASIN共{len(all_asin_data)}个"
            massage.append(info)
        print(massage)
        return massage
    else:
        return [f"品牌{brand} 国家{market}托管ASIN无小分类，无搜索词竞品ASIN"]


if __name__ == "__main__":
    asyncio.run(searchterm_asin('amazon_61_B3dianpu','M SHUYUN','US',60,3))
#     job()
#     brands_and_countries = {
#         'amazon_ads': {
#             'brand': 'LAPASA',
#             'countries': [ "US", "FR", "IT", "DE", "ES", "UK", "JP"]
#             # "US", "FR", "IT", "DE", "ES", "UK", "JP"
#         },
#         'amazon_huangjunxi': {
#             'brand': 'keimi',
#             'countries': ['AU','DE','UK']
#         },
#         'amazon_outdoormaster_jp': {
#             'brand': 'OutdoorMaster',
#             'countries': ["JP"]
#         },
#         'amazon_bdzx': {
#             'brand': 'DELOMO',
#             'countries': ['IT', 'ES', 'DE', 'FR']
#         },
#         'amazon_bdzx_delomo': {
#             'brand': 'DELOMO',
#             'countries': ['US']
#         },
#         'amazon_52_ColorNymph': {
#             'brand': '5-Star',
#             'countries': ['US']
#         },
#         'amazon_outdoormaster': {
#             'brand': 'OutdoorMaster',
#             'countries': ['IT', 'ES', 'FR']
#         }
#
#     }
#     for key, value in brands_and_countries.items():
#         brand = value.get('brand', value['brand'])  # 读取 'brand'
#         countries = value['countries']
#         for country in countries:
#             main(key, brand, country)
