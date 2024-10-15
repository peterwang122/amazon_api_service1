import asyncio
import functools
import aiohttp
import json
from functools import wraps
from time import sleep
import random
from collections.abc import Iterable
from ad_api.base import Marketplaces
from logs.logger import logger
from util.common import get_ad_my_credentials
from concurrent.futures import ThreadPoolExecutor
from util.proxies import ProxyManager



# 假设你的基础API类
class BaseApi:
    def __init__(self, db, brand, market):
        self.brand = brand
        self.market = market
        self.db = db
        self.executor = ThreadPoolExecutor()
        self.credentials, self.access_token = self.load_credentials()
        self.attempts_time = 3
        self.proxy_manager = ProxyManager()

    def load_credentials(self):
        # 假设这个方法是通用的，可以直接在这里实现
        my_credentials, access_token = get_ad_my_credentials(self.db, self.market, self.brand)
        return my_credentials, access_token

    def log(self, message):
        logger.info(message)

    async def wait_time(self):
        wait_time = random.randint(5, 10)
        self.log(f"Waiting for {wait_time} seconds before retrying...")
        await asyncio.sleep(wait_time)

    async def make_request(self, api_class, method_name, *args, **kwargs):
        attempts = 0
        result = None
        while attempts < self.attempts_time:
            try:
                self.log(
                    f"Attempting to call {method_name} with args: {args}, kwargs: {kwargs}. Attempt {attempts + 1}")

                # 动态创建 API 类的实例
                api_instance = api_class(
                    credentials=self.credentials,
                    marketplace=Marketplaces[self.market.upper()],
                    access_token=self.access_token,
                    proxies=self.proxy_manager.get_proxies(self.market),
                    debug=True
                )

                # 获取目标方法
                method = getattr(api_instance, method_name)

                # 异步执行同步方法
                result = await asyncio.get_event_loop().run_in_executor(
                    self.executor,functools.partial(method, **kwargs)
                )

                if result and result.payload:
                    self.log(f"{method_name} success. Payload: {result.payload}")
                    return result.payload
                else:
                    self.log(
                        f"{method_name} failed or returned invalid payload: {result.payload if result else 'None'}")
                    await self.wait_time()
                    res = result.payload
                    attempts += 1

            except Exception as e:
                self.log(f"Exception occurred in {method_name}: {e}")
                await self.wait_time()
                res = e
                attempts += 1
        return res

    def to_iterable(self, obj):
        if isinstance(obj, Iterable) and not isinstance(obj, (str, bytes)):
            return obj  # 如果是可迭代的（非字符串或字节），返回原对象
        else:
            return [obj]