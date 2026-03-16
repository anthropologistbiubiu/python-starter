import elasticsearch


# 精准查询


def term_query():
    # 精确单值查询
    es = elasticsearch.Elasticsearch([{'host':'localhost','port':9200}])
    body = {
        "query": {
            "terms": {
                "title.keyword": ["Python Elasticsearch Tutorial"]
            }
        }
    }
    print(es.search(index='my_index1', body=body))
def main():
   term_query()


if __name__ == '__main__':
    main()