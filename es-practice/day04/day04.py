import elasticsearch


def main():
    es = elasticsearch.Elasticsearch([{'host':'localhost','port':9200}])
    # es.delete(index='my_index1',id=1)
    print(es.count(index='my_index1')) # 获取该索引的所有文档数量
    indices = es.indices.get_alias("*")  # 获取所有的索引
    for index in indices:
        print(index)

if __name__ == '__main__':
    main()