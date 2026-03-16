import elasticsearch


def main():
    es = elasticsearch.Elasticsearch([{'host':'localhost','port':9200}])
    docs = [
        {'title':'c++','content':'c++'},
        {'title': 'java','content':'java'},
    ]
    es.bulk(index='my_index1',doc_type='my_doc1',body=docs)
    pass

if __name__ == '__main__':
    main()