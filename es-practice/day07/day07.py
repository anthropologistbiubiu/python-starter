

# 明天早上完成 es 的搜索，精准匹配，模糊匹配。

'''
multi_match，多字段查询
prefix，前缀查询
wildcard，通配符查询
regexp，正则匹配
bool，多条件查询
'''
import elasticsearch


# 用body指定条件
def body_search():
    es = elasticsearch.Elasticsearch([{'host':'localhost','port':9200}])
    body = {
       'from' :0,
        'size' :10
    }
    print(es.search(index='my_index1',body=body))


def filter():
    es = elasticsearch.Elasticsearch([{'host':'localhost','port':9200}])
    # 有过滤字段查询数据
    body = {
        'from': 0,  # 从0开始
    }
    # 定义过滤字段，最终只显示此此段信息 hits.hits._source.写在前面 后面写你自己定义的字段名 我这里是keyword和content
    filter_path = ['hits.hits._source.title',  # keyword为第一个需要显示的字段
                   'hits.hits._source.content']  # content为字段2
    print(es.search(index='my_index',filter_path=filter_path))

def main():
    filter()
    # body_search()


if __name__ == '__main__':
    main()
