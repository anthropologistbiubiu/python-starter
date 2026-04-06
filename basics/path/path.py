import os





def path_operation():
    abs = os.path.abspath("./basic/path")
    print(abs)
    # exists ：判断文件或目录是否存在
    exist = os.path.exists("./basic")
    print(exist)
    # os.path.isdir("test")
    isdir = os.path.isdir("./basic/day01/day01.py")
    print(isdir)

    # join ：合成路径，即把两个参数使用系统路径分割符进行连接，形成完整路径。
    dirs = os.path.join("test", "test-1")  # 连接两个目录
    print(dirs)
    dir_file = os.path.join("./basic/path","day01.py")
    print(dir_file)
    # split ：分割文件名和文件夹，即把 path 以最后一个斜线"/"为分隔符，切割为 head 和 tail ，以 (head, tail) 元组的形势返回。
    split_dirs = os.path.split("./basic/path/day01.py")
    print(split_dirs)



def main():
    path_operation()

if __name__ == '__main__':
    main()