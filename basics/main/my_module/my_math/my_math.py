
from .. import my_module
class mathTool:
    def __init__(self):
        pass
    def sum(self, *args):
        result = 0
        for num in args:
            result += num
        return result
    def sub_my_module(self,num1,num2):
        my_module_sum = my_module.Sub()
        return my_module_sum.sub(num1,num2)
