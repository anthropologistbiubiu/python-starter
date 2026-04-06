# 总结一下python 中不同数据类型与json 的序列化与反序列化
import pickle
import json

# 元祖的序列化
tup = (1,2,3)
print(json.dumps(tup))
# 列表的序列化
my_list = ['a','b','c']
print(json.dumps(my_list))
# dict 的序列化
my_dict = {"name":"sunweiming","age":18}
print(json.dumps(my_dict))

# 数值类型
numeric_data = 42
# 序列化
serialized_data = json.dumps(numeric_data)
print(serialized_data,type(serialized_data))
# 反序列化
deserialized_data = json.loads(serialized_data)
print(deserialized_data,type(deserialized_data))

# 文本类型
text_data = "Hello, World!"
# 序列化
serialized_data = json.dumps(text_data)
print(serialized_data,type(serialized_data))
# 反序列化
deserialized_data = json.loads(serialized_data)
print(deserialized_data,type(deserialized_data))


class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

person_object = Person("Bob", 30)

# 使用 pickle 序列化
pickle_serialized_data = pickle.dumps(person_object)
print(pickle_serialized_data ,type(pickle_serialized_data))
# 反序列化
pickle_deserialized_data = pickle.loads(pickle_serialized_data)

print(pickle_deserialized_data.name,type(pickle_deserialized_data.name))
print(pickle_deserialized_data.age,type(pickle_deserialized_data.age))

# 序列化对象为json
def encode_json(obj):
    if isinstance(obj,Person):
        return {"name":obj.name,"age":obj.age}
    return None

json_person_object = json.dumps(person_object,default=encode_json,indent=2)
print(json_person_object,type(json_person_object))
# 反序列化json 为对象

json_data = '{"name": "Bob", "age": 30}'
def decode_json(obj):
    if 'name' in obj and 'age' in obj:
        print(type(obj))
        return Person(obj['name'],obj['age'])
    return None


bob_object = json.loads(json_data, object_hook=decode_json)
print(bob_object,type(bob_object))