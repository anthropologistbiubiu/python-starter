# 解决python 异常处理的语法

# try...except...
def zero_division_Error():
    try:
        print(10 / 0)
    except ZeroDivisionError as e:
        print('ZeroDivisionError:', e)
    finally:
        print('zero_division_Error')

def value_Err():
    try:
        num = int(input())
        print('num',num)
    except ValueError as e:
        print('value_Err ', e)
    finally:
        print('value_Err')

def value_division_Err():
    try:
        num1 = int(input())
        num2 = int(input())
        reslut = num1/num2
        print('result',reslut)
    except (ZeroDivisionError,ValueError) as e:
        print('value_division_Err:',e)
    finally:
        print('value_division_Err,Fail')




#  自定义error
class CustomError(Exception):
    def __init__(self,message):
        self.message = message
        super().__init__(self.message)

def check_params(num1,num2):
    if num1 < 0 or num2 < 0:
        raise CustomError('num is nagetive')


def custom_Err():
    try:
        num1 = int(input())
        num2 = int(input())
        check_params(num1,num2)
        result = num1 / num2
        print(result)
    except (CustomError,ValueError,ZeroDivisionError) as e:
        print('custom_Err Fail:',e)
    else:
        print('custom_Err Success')

def io_Err():
    try:
        file = open('test.txt','r')
    except (IOError) as e:
        print('open file Fail:',e)
    else:
        for line in file:
            print(line,end='')
        file.close()


if __name__ == '__main__':
    io_Err()
    num = int(input())
    assert num < 0,"num is less than num"
