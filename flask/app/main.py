# import requests
import os
from flask import Flask,request,jsonify
app = Flask(__name__)

# 假设这是后端的数据
data = {
    "name": "John Doe",
    "age": 30,
    "city": "New York"
}

@app.route('/api/data', methods=['GET'])
def get_data():
    # 获取GET请求中的query参数
    name = request.args.get('name')
    age = request.args.get('age')
    print(name,age)
    # 进行数据处理或业务逻辑
    # ...
    # 返回处理后的结果
    result = {
        "name": name,
        "age": age
    }
    return jsonify(result)
# curl "http://127.0.0.1:8080/api/data?name=John&age=30"
@app.route('/api/data', methods=['POST'])
def update_data():
    new_data = request.get_json()  # 获取POST请求中的JSON数据
    print(new_data)
    return jsonify(new_data)

# curl localhost:8080/api/data -X POST -d '{"hello": "world"}' --header "Content-Type: application/json"



@app.route('/submit/x-www-form-urlencode', methods=['POST'])
def submit_xwwwformurlencod():
    # Get form parameters
    name = request.form.get('name')
    email = request.form.get('email')
    message = request.form.get('message')
    # Perform data processing or business logic here (if needed)
    # ...
    # Prepare response data
    response_data = {
        "name": name,
        "email": email,
        "message": message
    }
    return jsonify(response_data)
# curl -X POST -d "name=John&email=john@example.com&message=Hello" http://127.0.0.1:8080/submit


@app.route('/submit/formdata', methods=['POST'])
def submit_formdata():
    # Get form parameters
    name = request.form.get('name')
    email = request.form.get('email')
    message = request.form.get('message')

    # Perform data processing or business logic here (if needed)
    # ...

    # Prepare response data
    response_data = {
        "name": name,
        "email": email,
        "message": message
    }

    return jsonify(response_data)
# curl -X POST -F "name=sunweiming" -F "email=1319847957qq.com" -F "message=Hello" http://127.0.0.1:8080/submit/formdata



@app.route('/file/upload', methods=['POST'])
def upload_file():

    UPLOAD_FOLDER = './flask'  # 设置保存上传文件的目录
    app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

    if 'file' not in request.files:
        return "No file part in the request.", 400
    file = request.files['file']
    if file.filename == '':
        return "No selected file.", 400
    # 保存上传的文件
    if not os.path.exists(app.config['UPLOAD_FOLDER']):
        os.makedirs(app.config['UPLOAD_FOLDER'])

        # 保存上传的文件到指定目录
    file.save(os.path.join(app.config['UPLOAD_FOLDER'], file.filename))

    file.save(file.filename)
    return "File uploaded successfully!", 200


if __name__ == '__main__':
    app.run(debug=True,port=8080)
