# import Exception

# 只读（“r”）：这种模式以只读模式打开文本文件。文件的开始是句柄所在的位置。如果文件不存在，它将引发 I/O 错误。这也是打开文件的默认模式。
# 读和写（'r+’）：这种方法以读和写模式打开文件。文件的开始是句柄所在的位置。如果文件不存在，就会产生一个 I/O 错误。

def r_open():
    f = open("myfile.txt", "r")
    # str = f.read()
    strline1 = f.readlines()
    # strline2 = f.readline()
    print(strline1)
    # print(strline2)

def rw_open():
    f = open("myfile.txt", "r+")
    # f.write("hello world3\n")
    # f.write("hello world4\n")
    try:
        strLines = f.readlines()
        print(strLines)
    except IOError as e:
        print(e)
def w_open():
    try:
        f = open("myfile.txt", "w")
        n = f.write("hello world4")
        print("n",n)
        content = f.read()
        print(content)
    except IOError as e:
        print(e)
def wr_open():
    f = open("myfile.txt", "w+")
    try:
        #f.write("good study")
        # f.seek(0)
        str = f.read()
        print(str)
    except IOError as e: # 这种模式可以打开文件进行读和写。文本被覆盖并从现有文件中删除。文件的开始是句柄所在的位置。
        print(e)


def a_open():
    f = open("myfile.txt", "a")
    try:
        n = f.write(" hello world4\n")
        print(n)
        content = f.readlines()
        print(content)
    except IOError as e :
        print(e)

def aw_open():
    f = open("myfile.txt", "a+")
    try:
        n = f.write(" good study \n")
        print(n)
        f.close()
        f1 = open("myfile.txt","a+")
        content = f1.readline()  # 因为每次指针的偏移量都在最后一个职位
        print(content) # 句柄被设置在文件的末尾。新写的文本将被添加到最后，跟在先前写的数据后面。
    except IOError as e:
        print(e)

if __name__ == '__main__':
    rw_open()
