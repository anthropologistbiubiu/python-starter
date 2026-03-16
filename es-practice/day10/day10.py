import elasticsearch


def query_by_multil_field():
    es = elasticsearch.Elasticsearch([{'host':'localhost','port':9200}])
    # 查询多个字段中都包含指定内容的数据
    body = {
        "query": {
            "multi_match": {
                "query": "python",  # 指定查询内容，注意：会被分词
                "fields": ["title", "content"]  # 指定字段
            }
        }
    }
    print(es.search(index='my_index', body=body))
    pass


def main():
    query_by_multil_field()



if __name__ == '__main__':
    main()