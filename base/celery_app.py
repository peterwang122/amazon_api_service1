from celery import Celery

# 创建 Celery 应用实例
celery = Celery('tasks', broker='redis://192.168.2.165:6379')

# 配置 Celery
celery.conf.update(
    result_backend='redis://192.168.2.165:6379'
)





