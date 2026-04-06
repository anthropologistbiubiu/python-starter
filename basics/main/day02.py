import sys
sys.path.append('/Users/biubiu/Documents/python_study/mymodule')
from mymodule.my_addition import addtion




if __name__ == '__main__':
    add_tool = addtion.Addtion(1, 2, 3, 4, 5)
    print(add_tool.sum())
    print(add_tool.nums)

