import elasticsearch


def get_doc():
    es = elasticsearch.Elasticsearch([{"host":"127.0.0.1","port":9200,"scheme":"https"}])
    if es.ping():
       res = es.get(index="my_index1",doc_type="my_doc1",id=0)
       print("res",res)
    pass

def main():
    get_doc()

if __name__ == '__main__':
    main()