


import os


def main():
   dir_list = os.listdir()  # 获取文件夹及目录列表
   print(dir_list)
   pwd = os.getcwd()  # 获取当前程序执行的目录
   res = os.stat(".//basic/dir/day02") # 获取当前目录或文件信息
   print(res)
   print(pwd)
   data = os.walk("./basic/dir")

   for root,dir,file in data: # 目录的遍历
       print(root,dir,file)

   # os.mkdir("test")
   try:
      os.rmdir("test")
   except FileExistsError as e:
      print("FileExistsError ",e)
   except FileNotFoundError as e:
      print("FileNotFoundError ",e)
   except Exception as e:
      print("Exception ",e)
   # rename ：重命名目录或文件，可修改文件或目录的路径（即移动操作），若目标文件目录不存在，则报错。
   # renames ：重命名目录或文件，若目标文件目录不存在，则自动创建
   print(os.rename("./basic/dir/day3", "./basic/dir/day01"))



if __name__ == '__main__':
    main()