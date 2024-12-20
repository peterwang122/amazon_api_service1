import atexit
import json
import os
import subprocess
import threading
from datetime import datetime
from flask import Flask, request, jsonify, g
from logs.logger import logger
import hashlib
import time
from models.update_api import update_api
from models.create_api import create_api
from models.list_api import list_api
from apscheduler.schedulers.background import BackgroundScheduler
from util.automatic_configuration import automatic_configuration
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from models.update_api import update_api
# from ai.backend.util.db.auto_process.provide_api.util.create_api import create_api
# from ai.backend.util.db.auto_process.provide_api.util.automatically_api import automatically_api
from configuration.path import get_config_path

app = Flask(__name__)


# 定义你想定时执行的函数
def scheduled_task():
    # 这里放入你想定时执行的代码
    automatic_configuration()
    app.logger.info("automatic_configuration is running...")


# 设置调度器
scheduler = BackgroundScheduler()
# 添加定时任务，比如每10秒执行一次
scheduler.add_job(scheduled_task, 'interval', seconds=60 * 60 * 1)
scheduler.start()

# 确保在应用关闭时停止调度器
atexit.register(lambda: scheduler.shutdown())


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

# 用于缓存发送次数和时间的字典
error_cache = {}

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


def send_error_email(error_message, method_name):
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
    if response.status_code != 200:
        # 获取 JSON 数据
        response_data = g.request_data['data']

        # 如果 response_data 是字符串形式的 JSON，先解析为字典
        if isinstance(response_data, str):
            response_data = json.loads(response_data)

        # 格式化 JSON 数据，确保中文显示正常
        formatted_data = json.dumps(response_data, ensure_ascii=False)
        error_message = f"Request to {g.request_data['url']} failed with status {response.status_code}.\nResponse Text: {response.get_data(as_text=True)}\ndata:{formatted_data}"
        method_name = f"{g.request_data['method']} {g.request_data['url']}"
        # 调用 send_error_email 方法发送错误邮件
        send_error_email(error_message, method_name)
    return response

@app.route('/api/data/list', methods=['POST'])
def handle_list():
    # 获取请求头和请求体
    token = request.headers.get('token')
    timestamp = request.headers.get('timestamp')
    data = request.get_json()
    print(data)
    # 验证请求头
    secret_key = "69c5fcebaa65b560eaf06c3fbeb481ae44b8d618"  # 测试环境的秘钥, 根据环境配置选择秘钥
    if not verify_request(token, timestamp, secret_key):
        return jsonify({"error": "Unauthorized"}), 401
    if not data.get("text") or data["text"] == "":
        return jsonify({"status": 404, "error": "The 'text' field cannot be an empty string."})
    result = list_api(data)  # 获取 AsyncResult 对象
    code, info, e = result  # 从结果中解包任务返回的值（同步阻塞，等待任务完成）
    if code == 200:
        return jsonify({"status": 200, "info": info, "error": e})
    else:
        # 获取 JSON 数据
        response_data = g.request_data['data']

        # 如果 response_data 是字符串形式的 JSON，先解析为字典
        if isinstance(response_data, str):
            response_data = json.loads(response_data)

        # 格式化 JSON 数据，确保中文显示正常
        formatted_data = json.dumps(response_data, ensure_ascii=False)
        error_message = f"Request to {g.request_data['url']} failed .\nResponse data:{formatted_data}"
        method_name = f"{g.request_data['method']} {g.request_data['url']}"
        # 调用 send_error_email 方法发送错误邮件
        send_error_email(error_message, method_name)
        return jsonify({"status": code, "info": info, "error": str(e)})

@app.route('/api/data/create', methods=['POST'])
def handle_insert():
    # 获取请求头和请求体
    token = request.headers.get('token')
    timestamp = request.headers.get('timestamp')
    data = request.get_json()
    print(data)
    # 验证请求头
    secret_key = "69c5fcebaa65b560eaf06c3fbeb481ae44b8d618"  # 测试环境的秘钥, 根据环境配置选择秘钥
    if not verify_request(token, timestamp, secret_key):
        return jsonify({"error": "Unauthorized"}), 401
    if not data.get("text") or data["text"] == "":
        return jsonify({"status": 404, "error": "The 'text' field cannot be an empty string."})
    result = create_api(data)  # 获取 AsyncResult 对象
    code, id, e = result  # 从结果中解包任务返回的值（同步阻塞，等待任务完成）
    if code == 200:
        return jsonify({"status": 200, "id": id, "error": e})
    else:
        # 获取 JSON 数据
        response_data = g.request_data['data']

        # 如果 response_data 是字符串形式的 JSON，先解析为字典
        if isinstance(response_data, str):
            response_data = json.loads(response_data)
        print(response_data)
        if 'require' in response_data and response_data['require'] == 'create' and response_data['position'] == 'sku':
            print(111)
            print(error_cache)
            current_time = time.time()
            cache_key = f"{g.request_data['url']}_create_error"

            # 检查缓存是否存在
            if cache_key in error_cache:
                error_info = error_cache[cache_key]
                # 判断是否在 30 分钟内且发送次数未超过 3 次
                if (current_time - error_info['last_sent_time']) < 1800 and error_info['count'] < 3:
                    # 更新计数和时间
                    print("error_info['count']:"+str(error_info['count']))
                    error_info['count'] += 1
                    error_info['last_sent_time'] = current_time
                    error_cache[cache_key] = error_info
                elif (current_time - error_info['last_sent_time']) >= 1800:
                    # 30 分钟过后重置计数和时间
                    error_cache[cache_key] = {'count': 1, 'last_sent_time': current_time}
                else:
                    return jsonify({"status": code, "error": str(e)})
            else:
                # 添加到缓存中，首次发送
                error_cache[cache_key] = {'count': 1, 'last_sent_time': current_time}
        # 格式化 JSON 数据，确保中文显示正常
        formatted_data = json.dumps(response_data, ensure_ascii=False)
        error_message = f"Request to {g.request_data['url']} failed .\nResponse data:{formatted_data}"
        method_name = f"{g.request_data['method']} {g.request_data['url']}"
        # 调用 send_error_email 方法发送错误邮件
        send_error_email(error_message, method_name)
        return jsonify({"status": code, "id": id, "error": str(e)})


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
    result = update_api(data)  # 获取 AsyncResult 对象
    code, e = result  # 从结果中解包任务返回的值（同步阻塞，等待任务完成）
    if code == 200:
        return jsonify({"status":200,"error":e})
    else:
        # 获取 JSON 数据
        response_data = g.request_data['data']

        # 如果 response_data 是字符串形式的 JSON，先解析为字典
        if isinstance(response_data, str):
            response_data = json.loads(response_data)
            # 检查是否需要发送错误邮件
        if 'require' in response_data and response_data['require'] == 'create':
            print(111)
            print(error_cache)
            current_time = time.time()
            cache_key = f"{g.request_data['url']}_create_error"

            # 检查缓存是否存在
            if cache_key in error_cache:
                error_info = error_cache[cache_key]
                # 判断是否在 30 分钟内且发送次数未超过 3 次
                if (current_time - error_info['last_sent_time']) < 1800 and error_info['count'] < 3:
                    # 更新计数和时间
                    print("error_info['count']:"+str(error_info['count']))
                    error_info['count'] += 1
                    error_info['last_sent_time'] = current_time
                    error_cache[cache_key] = error_info
                elif (current_time - error_info['last_sent_time']) >= 1800:
                    # 30 分钟过后重置计数和时间
                    error_cache[cache_key] = {'count': 1, 'last_sent_time': current_time}
                else:
                    return jsonify({"status": code, "error": str(e)})
            else:
                # 添加到缓存中，首次发送
                error_cache[cache_key] = {'count': 1, 'last_sent_time': current_time}
        # 格式化 JSON 数据，确保中文显示正常
        formatted_data = json.dumps(response_data, ensure_ascii=False)
        error_message = f"Request to {g.request_data['url']} failed .\nResponse data:{formatted_data}"
        method_name = f"{g.request_data['method']} {g.request_data['url']}"
        # 调用 send_error_email 方法发送错误邮件
        send_error_email(error_message, method_name)
        return jsonify({"status":code,"error": str(e)})


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


# @app.route('/api/data/get_data', methods=['GET'])
# def get_data():
#     # 验证请求头
#     token = request.headers.get('token')
#     timestamp = request.headers.get('timestamp')
#     secret_key = "69c5fcebaa65b560eaf06c3fbeb481ae44b8d618"  # 测试环境的秘钥
#     if not verify_request(token, timestamp, secret_key):
#         return jsonify({"error": "Unauthorized"}), 401
#
#     data = request.get_json()
#     # 读取 execution_times.json 文件
#     execution_path = os.path.join(get_config_path(), f'{data["file"]}.json')
#     if os.path.exists(execution_path):
#         with open(execution_path, 'r') as json_file:
#             execution_times = json.load(json_file)
#             return jsonify(execution_times), 200
#     else:
#         return jsonify({"error": "File not found"}), 404

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
    # worker_process = None
    # flower_process = None
    # def start_celery():
    #     global worker_process, flower_process
    #     worker_command = ['celery', '-A', 'celery_app.celery', 'worker', '--loglevel=info']
    #     flower_command = ['celery', '-A', 'celery_app.celery', 'flower']
    #     worker_process = subprocess.Popen(worker_command)
    #     flower_process = subprocess.Popen(flower_command)
    # def stop_celery():
    #     if worker_process:
    #         worker_process.terminate()
    #     if flower_process:
    #         flower_process.terminate()
    # atexit.register(stop_celery)
    # start_celery()
    app.run(debug=False, host='0.0.0.0', port=8008, threaded=True)
