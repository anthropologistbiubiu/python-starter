
def add(a, b):
    c = a ^ b
    d = (a & b) << 1
    while d != 0:
        a = c
        b = d
        c = a ^ b
        d = (a & b) << 1
    return c

# 使用位运算实现减法
def sub(num1,num2):
    num2 = add(~num2, 1)
    print(num2)
    return add(num1, num2)

def main():
    print(sub(10,1))


if __name__ == '__main__':
    main()