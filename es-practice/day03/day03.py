import elasticsearch


# 更细文档



def update_doc():
    es = elasticsearch.Elasticsearch([{"host":"localhost","port":9200,"schem":"http"}])
    '''
    print(es.get(index="my_index1",doc_type="my_doc1",id=0))
    body = {
        'title':'sunweiming',
        'content':'good man',
    }
    es.index(index='my_index1',doc_type='my_doc1',id=1,body=body)
    print(es.get(index="my_index1",doc_type="my_doc1",id=1))
    '''
    update_body = {
        'title':'yujinling',
        'content':'good man',
    }
    # es.update(index="my_index1",id=1,body={'doc':{'title':'python'}})
    print(es.get(index="my_index1",doc_type="my_doc1",id=1))

def main():
    update_doc()
    pass

if __name__ == '__main__':
    main()