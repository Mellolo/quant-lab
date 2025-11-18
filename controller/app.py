from flask import Flask, jsonify, request
from flask_socketio import SocketIO, emit, disconnect
from io import StringIO
import pandas as pd
import service.backService as backService
import threading
from typing import Dict

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

back_space_sids: Dict[str, int] = {}
bs_lock = threading.Lock()
back_space_locks: Dict[int, threading.Lock] = {}

@app.route('/api/create/astock', methods=['POST'])
def create():
    # 检查是否有文件在请求中
    if 'file' not in request.files:
        return jsonify({"message": "创建回测空间失败，未上传行情csv文件"}), 400

    csv_file = request.files['file']
    csv_string = csv_file.stream.read().decode('utf-8')
    csv_data = StringIO(csv_string)
    df = pd.read_csv(csv_data)

    backService.create_back_space_astock(df)

    return jsonify({"message": "成功创建回测空间"}), 201

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

@app.route('/health', methods=['GET'])
def health_check():
    """
    健康检查接口
    """
    return jsonify({"status": "healthy"})

@socketio.on('connect')
def handle_connect(space_id: int):
    # 单例模式创建锁
    if space_id not in back_space_locks:
        bs_lock.acquire()
        if space_id not in back_space_locks:
            back_space_locks[space_id] = threading.Lock()

    # 锁定回测空间的连接
    acquired = back_space_locks[space_id].acquire(blocking=False)
    if not acquired:
        emit('connection_rejected', {'reason': '该回测空间已在运行中，无法打开'})
        disconnect(request.sid)  # 断开连接
    # 将该连接绑定回测空间
    back_space_sids[request.sid] = space_id

@socketio.on('disconnect')
def handle_disconnect():
    if request.sid in back_space_sids and back_space_sids[request.sid] in back_space_locks:
        back_space_locks[back_space_sids[request.sid]].release()

@socketio.on('message')
def handle_message(data):
    """
    处理客户端发送的消息
    """
    print('Received message: ' + str(data))
    emit('response', {'data': f"Server received: {data}"})

@socketio.on('json')
def handle_json(json_data):
    """
    处理客户端发送的JSON数据
    """
    print('Received json: ' + str(json_data))
    emit('response', {'data': f"Server processed JSON: {json_data}"})

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=9990, debug=True)