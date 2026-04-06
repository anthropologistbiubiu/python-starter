
# 位运算符表示➕➖✖️➗

# 也就是说依然没有解决的是正数和负数之间的求和

# 负数&负数一定是负数
# 正数
def add(a, b):
    c = a ^ b
    d = (a & b) << 1
    print(c,d)
    while d != 0:
        a = c
        b = d
        c = a ^ b
        d = (a & b) << 1
    return c


def main():
    print(bit_operator_add(1,-1))
if __name__ == '__main__':
  main()

