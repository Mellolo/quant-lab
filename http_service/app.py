from flask import Flask, jsonify, request
from flask_socketio import SocketIO, emit

app = Flask(__name__)
#app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, cors_allowed_origins="*")

@app.route('/api/example', methods=['GET'])
def example_endpoint():
    """
    示例GET接口，返回JSON字符串
    """
    response_data = {
        "message": "Hello, World!",
        "status": "success",
        "data": {
            "example_key": "example_value",
            "timestamp": "2025-11-08T12:00:00Z"
        }
    }
    return jsonify(response_data)

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
def handle_connect():
    """
    处理客户端连接事件
    """
    print('Client connected')
    emit('response', {'data': 'Connected successfully'})

@socketio.on('disconnect')
def handle_disconnect():
    """
    处理客户端断开连接事件
    """
    print('Client disconnected')

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