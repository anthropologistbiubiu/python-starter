import elasticsearch


def query():
    es = elasticsearch.Elasticsearch([{'host':'localhost','port':9200}])
    body = {
        'from': 0,
        'query': {
            # 查询命令
            'match': {
                # 查询方法：模糊查询
                'content': 'Elasticsearch'  # content为字段名称，match这个查询方法只支持查找一个字段
            }
        }
    }

    filter_path = ['hits.hits._source.title',  # 字段1
                   'hits.hits._source.content']  # 字段2
    print(es.search(index='my_index', filter_path=filter_path, body=body))
    pass

def main():
    query()
if __name__ == '__main__':
    main()