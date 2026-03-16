import elasticsearch


## 通配符与正则表达式
def query():
    es = elasticsearch.Elasticsearch([{'host':'localhost','port':9200,'schem':'https'}])
    #body = {
    #    'query': {
    #        'wildcard': {
    #            'content.keyword': '?*'  # ?代表一个字符，*代表0个或多个字符
    #        }
    #    }
    #}
    body = {
        'query': {
            'regexp': {
                'content': '*'  # 使用正则表达式查询
            }
        }
    }
    condition = {
        'regexp' :{
            'content':'*'
        }
    }
    # 注：此方法只能查询单一格式的（都是英文字符串，或者都是汉语字符串）。两者混合不能查询出来。
    es.search(index='my_index1',query=condition)

def main():
    query()


if __name__ == '__main__':
    main()