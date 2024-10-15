# tasks.py
from main import celery  # 导入 Celery 实例
from models.update_api import update_api


# 定义一个简单的任务
@celery.task
def update_task(data):
    code,e = update_api(data)
    return code,e

# # 定义一个耗时的任务
# @celery.task
# def long_task():
#     import time
#     time.sleep(10)  # 模拟长时间执行
#     return "Task completed"
