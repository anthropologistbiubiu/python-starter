

from elasticsearch import Elasticsearch

# 连接到本地的Elasticsearch节点


def main():
    try:
        # es = Elasticsearch([{'host': '127.0.0.1', 'port': 9200}], timeout=3600)
        es = Elasticsearch([{"host":"localhost","port":9200,"scheme":"http"}])
        # con = es.ping()
        # print(con)
    except Exception as e:
        print("Exception",e)
        return
    if es.ping():
        # res = es.indices.create(index='my_index1')
        # print("es",res)
        doc = {
            'title': 'Python Elasticsearch Tutorial',
            'content': 'Elasticsearch is a powerful search engine and analytics engine.',
        }
        # 创建一个新的索引并插入文档
        print(es.index(index="my_index1",doc_type='my_doc1',id=0,body=doc))



if __name__ == '__main__':
    main()