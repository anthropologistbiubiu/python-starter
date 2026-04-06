

# 00110001
# 00000011
# -------------

def add(a,b):
    pass
def multiply(a,b):
  ans = 0
  for i in range (64):
    if b & 1 == 1:
        ans += (a << i)
    b = b >> 1
  return ans

def main():
    print(multiply(1,2001))

if __name__ == '__main__':
    main()