import elasticsearch


def prefix_query():
    es = elasticsearch.Elasticsearch([{'host':'localhost','port':9200}])
    body = {
        "query": {
            "prefix": {
                "content.keyword": "E",  # 查询前缀是指定字符串的数据
            }
        }
    }
    # 注：英文不需要加keyword
    print(es.search(index='my_index1', body=body))
    pass

def main():
    prefix_query()

if __name__ == '__main__':
    main()