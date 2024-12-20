import asyncio
import functools
import aiohttp
import json
from functools import wraps
from time import sleep
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
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
        self.attempts_time = 5
        self.proxy_manager = ProxyManager()

    def load_credentials(self):
        # 假设这个方法是通用的，可以直接在这里实现
        my_credentials, access_token = get_ad_my_credentials(self.db, self.market, self.brand)
        return my_credentials, access_token

    def log(self, message):
        logger.info(message)

    async def wait_time(self):
        wait_time = random.randint(10, 20)
        self.log(f"Waiting for {wait_time} seconds before retrying...")
        await asyncio.sleep(wait_time)

    async def send_error_email(self, error_message, method_name):
        """发送错误邮件"""
        sender_email = "wanghequan@deepbi.com"  # 你的飞书邮箱地址
        receiver_emails = ["wanghequan@deepbi.com", "lipengcheng@deepbi.com"]  # 收件人的邮箱地址列表
        password = "tpa15CVg4pfBL1yK"  # 你的飞书邮箱授权码
        smtp_server = "smtp.feishu.cn"  # 飞书的 SMTP 服务器地址
        smtp_port = 587  # 使用 TLS 加密（端口 587）

        subject = f"Error in {method_name} API Call"
        body = f"An error occurred in method {method_name}: {error_message}"

        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = ", ".join(receiver_emails)  # 多个收件人，用逗号分隔
        msg["Subject"] = subject

        msg.attach(MIMEText(body, "plain"))

        try:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()  # 启用 TLS 加密
                server.login(sender_email, password)
                server.sendmail(sender_email, receiver_emails, msg.as_string())  # 发送邮件到多个收件人
                print(f"Error email sent successfully to {', '.join(receiver_emails)}")
        except Exception as email_error:
            print(f"Failed to send error email: {str(email_error)}")

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
                if attempts == self.attempts_time - 1:
                    self.log(f"Exception occurred in {method_name}: {str(e)}")
                    # 如果是最后一次尝试，直接抛出异常
                    await self.send_error_email(str(e), method_name)
                    raise e
                else:
                    self.log(f"Exception occurred in {method_name}: {str(e)}")
                    i = 0
                    while i < attempts + 1:
                        await self.wait_time()
                        i += 1
                    attempts += 1
        return res

    def to_iterable(self, obj):
        if isinstance(obj, Iterable) and not isinstance(obj, (str, bytes)):
            return obj  # 如果是可迭代的（非字符串或字节），返回原对象
        else:
            return [obj]

if __name__ == "__main__":
    asyncio.run( BaseApi('amazon_ads','LAPASA','US').send_error_email("123","321"))