import http.client



def func():
    try:
        if Ture:  # NameError
            print("ok")
    except (RuntimeError, TypeError, NameError) as e:
       print("TypeError ",e)
    else:
        print("pass")




def diviedByZero():
    if 1/0:
        print("ok")

def webError():
    url = "wwww.baidu.com"
    try:
        conn = http.client.HTTPConnection(host="127.0.0.1",port=8888)
        conn.request("POST",url)
        response = conn.getresponse()
        if response.status == 200:
            print(response.data)
    except http.client.HTTPException as e:
        print("HTTPException   ", e)
    except Exception as e:
        print("Exception  ",e)
    else:
        print("Success")
    finally:
        print("nice")


def main():
    try:
        diviedByZero()
    except ArithmeticError as e:
        print("ArithmeticError",e)
    except ZeroDivisionError as e:
        print("ZeroDivisionError ",e)
    except:
        print("Error")
    func()
    webError()

if __name__ == '__main__':
    main()