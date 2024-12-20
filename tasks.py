# tasks.py
from celery_app import celery  # 导入 Celery 实例
from models.update_api import update_api
from models.create_api import create_api
from models.list_api import list_api


# 定义一个简单的任务
@celery.task
def update_task(data):
    code, e = update_api(data)
    return code, e


@celery.task
def create_task(data):
    code, id, e = create_api(data)
    return code, id, e

@celery.task
def list_task(data):
    code, info, e = list_api(data)
    return code, info, e

# # 定义一个耗时的任务
# @celery.task
# def long_task():
#     import time
#     time.sleep(10)  # 模拟长时间执行
#     return "Task completed"
