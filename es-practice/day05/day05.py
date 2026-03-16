import elasticsearch




def main():
    es = elasticsearch.Elasticsearch([{'host': 'localhost', 'port': 9200}])
    index_name = 'my_index'
# 查询所有数据
    query = {
        'query': {
            'match_all': {}
        }
    }

    # 执行查询
    result = es.search(index=index_name, body=query, size=10000)
    # 处理查询结果
    print("result",result)
    if result['hits']['total']['value'] > 0:
        data = result['hits']['hits']
        for item in data:
            print("data",item['_source'])
    else:
        print("索引中没有数据。")

if __name__ == '__main__':
    main()
