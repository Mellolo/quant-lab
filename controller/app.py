import json
from flask import Flask, jsonify, request
from flask_socketio import SocketIO, emit, disconnect
from io import StringIO
import pandas as pd
from typing import Dict

import service.backService as backService
from models.backSqace import BackSpaceConnect
from utils.idLock import IDLockManager

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

connect_lock = IDLockManager()
connect_map:Dict[str, BackSpaceConnect] = {}

@app.route('/api/create/astock', methods=['POST'])
def create():
    # 检查是否有文件在请求中
    if 'file' not in request.files:
        return jsonify({"message": "创建回测空间失败，未上传行情csv文件"}), 400

    name = request.form.get('name')
    csv_file = request.files['file']
    csv_string = csv_file.stream.read().decode('utf-8')
    csv_data = StringIO(csv_string)
    df = pd.read_csv(csv_data)

    space_id, _ = backService.create_back_space_astock(name, df)

    return jsonify(
        {
            "message": "成功创建回测空间",
            "id": space_id
        }), 201

@app.route('/api/data', methods=['POST'])
def post_data():
    """
    示例POST接口，接收并返回JSON数据
    """
    # 获取请求体中的JSON数据
    request_data = request.get_json()
    
    # 检查是否提供了数据
    if not request_data:
        return jsonify({"error": "No data provided"}), 400
    
    # 返回接收到的数据以及成功消息
    response_data = {
        "message": "Data received successfully",
        "status": "success",
        "received_data": request_data
    }
    return jsonify(response_data), 201

@app.route('/api/search', methods=['GET'])
def health_check():
    keyword = request.form.get("keyword", None)
    space_list = backService.search_back_space(keyword)
    response_data = {
        "data": [space.to_dict() for space in space_list],
        "status": "success",
    }
    return jsonify(response_data)

@socketio.on('connect')
def handle_connect(space_id: int):
    # 单例模式创建运行中的回测空间
    for connect in connect_map.values():
        if connect.space_id == space_id:
            emit('connection_rejected', {'reason': '该回测空间已在运行中，无法打开'})
            disconnect(request.sid)  # 断开连接
    # double check
    connect_lock.acquire_lock(space_id)
    for connect in connect_map.values():
        if connect.space_id == space_id:
            emit('connection_rejected', {'reason': '该回测空间已在运行中，无法打开'})
            disconnect(request.sid)  # 断开连接
            # 释放锁
            connect_lock.release_lock(space_id)

    # 创建运行中的回测空间
    back_space = backService.get_back_space(space_id)
    connect_map[request.sid] = BackSpaceConnect(sid=request.sid, space_id=space_id, engine=backService.create_back_engine_astock(back_space))

    # 释放锁
    connect_lock.release_lock(space_id)

@socketio.on('disconnect')
def handle_disconnect():
    connect = connect_map.get(request.sid, None)
    if connect is None:
        return

    # 加锁
    connect_lock.acquire_lock(connect.space_id)

    # double check
    connect_regain = connect_map.get(request.sid, None)
    if connect_regain is None:
        # 释放锁
        connect_lock.release_lock(connect.space_id)
        return

    # 关闭运行中的回测空间

    connect_map.pop(request.sid)

    # 释放锁
    connect_lock.release_lock(connect.space_id)

@socketio.on('signal')
def handle_message(signal):
    json.loads(signal)
    emit('response', {'data': f"Server received: {data}"})
