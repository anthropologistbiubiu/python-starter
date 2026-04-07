from flask import Flask,request,jsonify

# 定义动态路由


app = Flask(__name__)

@app.route('/user/login',methods=['POST'])  # 处理json 请求
def  login():
    try:
        json_data = request.get_json()
        print(json_data)
        response_data = {"status": "success", "message": "JSON data received successfully"}
        return jsonify(response_data)
    except Exception as e:
        print(f"Error: {e}")
        response_data = {"status": "error", "message": "An error occurred while processing the request"}
        return jsonify(response_data)

@app.route('/user/payment',methods=['POST'])
def user_payment(): # 用户使用userid 发起充值 接受一个post 表单请求。
    try:
        user_name = request.form.get('username')
        password = request.form.get('username')
        print(f'{user_name} {password}')
        response_data = {"status": "success", "message": "JSON data received successfully"}
        return jsonify(response_data)
    except Exception as e:
        print(f'Exception {e}')
        response_data = {"status": "error", "message": "An error occurred while processing the request"}
        return jsonify(response_data)

@app.route('/user/payout',methods=['POST'])
def user_payout():     # 用户使用userid 提现 接收一个form-data 请求
    try:
        uid = request.form.get('uid')
        amount = request.form.get('amount')
        print(f'{uid} {amount}')
        response_data = {"status": "success", "message": "JSON data received successfully"}
        return jsonify(response_data)
    except Exception as e:
        print(f'Exception {e}')
        response_data = {"status": "error", "message": "An error occurred while processing the request"}
        return jsonify(response_data)
@app.route('/order/status',methods=['GET'])
def get_order_status():  # 用户使用订单号查询订单状态 接收一个get 请求。
    try:
        orderid = request.args.get('orderid')
        print(f'orderid is {orderid}')
        response_data = {"status": "success", "message": "JSON data received successfully"}
        return jsonify(response_data)
    except Exception as e:
        print(f'Exception {e}')
        response_data = {"status": "error", "message": "An error occurred while processing the request"}
        return jsonify(response_data)


def main():
    app.run(host='127.0.0.1',port=8081,debug=True)
if __name__ == '__main__':
    main()


