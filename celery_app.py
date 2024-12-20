from celery import Celery

# 创建 Celery 应用实例
celery = Celery('main', broker='redis://127.0.0.1:6379')

# 配置 Celery
celery.conf.update(
    result_backend='redis://127.0.0.1:6379'
)

# 自动发现并注册任务
celery.autodiscover_tasks(['tasks'])  # 任务所在的模块






