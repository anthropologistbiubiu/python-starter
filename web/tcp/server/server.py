import socket
import time


def main():
    # 创建一个 TCP 套接字
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 绑定到指定端口
    server_address = ('', 8081)  # 空字符串表示监听所有可用的接口
    server_socket.bind(server_address)

    # 开始监听
    server_socket.listen(5)

    print("等待客户端连接...")

    while True:
        # 等待客户端连接
        client_socket, client_address = server_socket.accept()
        print(f"连接来自：{client_address}")

        # 设置连接超时为 10 秒
        client_socket.settimeout(10)

        try:
            # 接收并发送数据，持续 10 秒钟
            start_time = time.time()
            while time.time() - start_time < 10:
                data = client_socket.recv(1024)
                if not data:
                    break
                client_socket.sendall(data)
        except socket.timeout:
            print("连接超时")
        finally:
            # 关闭客户端连接
            client_socket.close()


if __name__ == "__main__":
    main()
