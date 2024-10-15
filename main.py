import json
import os
import subprocess
import threading
from datetime import datetime
from celery import Celery
from flask import Flask, request, jsonify, g
import flower
from logs.logger import logger
import hashlib
import time
from models.update_api import update_api
from base.celery_app import make_celery
# from ai.backend.util.db.auto_process.provide_api.util.create_api import create_api
# from ai.backend.util.db.auto_process.provide_api.util.automatically_api import automatically_api
from configuration.path import get_config_path

app = Flask(__name__)

# 配置 Celery
app.config.update(
    CELERY_BROKER_URL='redis://192.168.5.165:6379/0',  # 替换为你的 Redis 或 RabbitMQ 配置
    CELERY_RESULT_BACKEND='redis://192.168.5.165:6379/0'
)

celery = make_celery(app)

@celery.task
def update_task(data):
    code,e = update_api(data)
    return code,e

def run_flower():
    # 使用 subprocess 启动 Flower
    subprocess.Popen(['celery', '-A', 'main', 'flower'])

# 验证函数
def verify_request(token, timestamp, secret_key):
    # 计算token
    calculated_token = hashlib.sha256((secret_key + str(timestamp) + secret_key).encode('utf-8')).hexdigest()
    return token == calculated_token


def validate_id(data):
    """检查数据中的ID是否有效"""
    if not data or 'ID' not in data or not data['ID']:
        return False
    if 'user' not in data or not data['user']:
        return False
    if 'db' not in data or not data['db']:
        return False
    return True


@app.before_request
def before_request():
    # 记录请求开始时间
    g.start_time = time.time()
    # 记录请求的基本信息
    g.request_data = {
        'method': request.method,
        'url': request.url,
        'headers': dict(request.headers),
        'data': request.get_data(as_text=True)
    }
    logger.info(f"Request started: {g.request_data}")


@app.after_request
def after_request(response):
    # 计算请求处理时间
    elapsed_time = time.time() - g.start_time
    # 记录响应的基本信息
    log_data = {
        'method': g.request_data['method'],
        'url': g.request_data['url'],
        'status': response.status,
        'text': response.get_data(as_text=True),
        'elapsed_time': elapsed_time,
        'headers': g.request_data['headers'],
        'data': g.request_data['data']
    }
    logger.info(f"Request finished: {log_data}")
    return response


# @app.route('/api/data/create', methods=['POST'])
# def handle_insert():
#     # 获取请求头和请求体
#     token = request.headers.get('token')
#     timestamp = request.headers.get('timestamp')
#     data = request.get_json()
#     print(data)
#     # 验证请求头
#     secret_key = "10470c3b4b1fed12c3baac014be15fac67c6e815"  # 测试环境的秘钥, 根据环境配置选择秘钥
#     if not verify_request(token, timestamp, secret_key):
#         return jsonify({"error": "Unauthorized"}), 401
#     if not data.get("text") or data["text"] == "":
#         return jsonify({"status": 404, "error": "The 'text' field cannot be an empty string."})
#     code = create_api(data)
#     if code == 200:
#         return jsonify({"status": 200, "error": ""})
#     elif code == 404:
#         return jsonify({"status": 404, "error": "Resource not found"})
#     elif code == 500:
#         return jsonify({"status": 500, "error": "Internal Server Error"})
#     else:
#         return jsonify({"status": 404, "error": "Unknown error"})  # Bad Request


@app.route('/api/data/update', methods=['POST'])
def handle_update():
    # 获取请求头和请求体
    token = request.headers.get('token')
    timestamp = request.headers.get('timestamp')
    data = request.get_json()

    # 验证请求头
    secret_key = "69c5fcebaa65b560eaf06c3fbeb481ae44b8d618"  # 测试环境的秘钥, 根据环境配置选择秘钥
    if not verify_request(token, timestamp, secret_key):
        return jsonify({"status":401,"error":"Unauthorized"})
    if not validate_id(data):
        return jsonify({"status":400,"error":"Invalid or missing ID"})
    # 调用 update_api 并处理返回值
    code,e = update_task.delay(data)  # 异步执行任务
    if code == 200:
        return jsonify({"status":200,"error":e})
    elif code == 404:
        return jsonify({"status":400,"error": e})
    elif code == 500:
        return jsonify({"status":500,"error": e})
    else:
        return jsonify({"status":404,"error": e})  # Bad Request


@app.route('/api/data/delete', methods=['POST'])
def handle_delete():
    # 获取请求头和请求体
    token = request.headers.get('token')
    timestamp = request.headers.get('timestamp')
    data = request.get_json()

    # 验证请求头
    secret_key = "69c5fcebaa65b560eaf06c3fbeb481ae44b8d618"  # 测试环境的秘钥, 根据环境配置选择秘钥
    if not verify_request(token, timestamp, secret_key):
        return jsonify({"error": "Unauthorized"}), 401

    # 处理删除数据的逻辑
    # 在此处添加处理删除数据的逻辑
    return jsonify({"message": "Delete data received"}), 200


@app.route('/api/data/get_data', methods=['GET'])
def get_data():
    # 验证请求头
    token = request.headers.get('token')
    timestamp = request.headers.get('timestamp')
    secret_key = "69c5fcebaa65b560eaf06c3fbeb481ae44b8d618"  # 测试环境的秘钥
    if not verify_request(token, timestamp, secret_key):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json()
    # 读取 execution_times.json 文件
    execution_path = os.path.join(get_config_path(), f'{data["file"]}.json')
    if os.path.exists(execution_path):
        with open(execution_path, 'r') as json_file:
            execution_times = json.load(json_file)
            return jsonify(execution_times), 200
    else:
        return jsonify({"error": "File not found"}), 404

# @app.route('/api/data/automatically', methods=['POST'])
# def get_automatically():
#     # 验证请求头
#     token = request.headers.get('token')
#     timestamp = request.headers.get('timestamp')
#     secret_key = "10470c3b4b1fed12c3baac014be15fac67c6e815"  # 测试环境的秘钥
#     if not verify_request(token, timestamp, secret_key):
#         return jsonify({"error": "Unauthorized"}), 401
#
#     data = request.get_json()
#     code = automatically_api(data)
#     current_time = time.time()
#     if code == 200:
#         return jsonify({"status": 200, "error": "", "timestamp": datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S')})
#     elif code == 404:
#         return jsonify({"status": 404, "error": "Brand not found"})
#     elif code == 500:
#         return jsonify({"status": 500, "error": "Internal Server Error"})
#     else:
#         return jsonify({"status": 404, "error": "Unknown error"})  # Bad Request


if __name__ == '__main__':
    def run_celery():
        celery.worker_main()

    # 启动 Celery worker 线程
    thread = threading.Thread(target=run_celery)
    thread.start()

    # 启动 Flower 线程
    flower_thread = threading.Thread(target=run_flower)
    flower_thread.start()
    app.run(debug=False, host='0.0.0.0', port=8888, threaded=True)
