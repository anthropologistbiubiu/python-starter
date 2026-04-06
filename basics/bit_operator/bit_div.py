# 180 /25 向下整除

def division(a,b):
    ans = 0
    for i in range(63, -1, -1):
        if a - b * (1 << i) >= 0:
            a = a - b * (1 << i)
            ans += (1 << i)
    return ans


def main():
   print(division(180,25))

if __name__ == '__main__':
    main()