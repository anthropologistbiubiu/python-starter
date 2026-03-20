
# app/services/es_demo_service.py
from app.config.settings import settings
from app.client.es_client import get_es_client


INDEX_BODY = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {
            "order_id": {"type": "keyword"},
            "customer_name": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}},
            },
            "status": {"type": "keyword"},
            "country": {"type": "keyword"},
            "amount": {"type": "double"},
            "remark": {"type": "text"},
            "created_at": {"type": "date"},
        }
    },
}


def create_index():
    if get_es_client().indices.exists(index=settings.ORDER_INDEX):
        return {"message": "index already exists"}
    return get_es_client().indices.create(index=settings.ORDER_INDEX, body=INDEX_BODY)


def delete_index():
    if not get_es_client().indices.exists(index=settings.ORDER_INDEX):
        return {"message": "index not exists"}
    return get_es_client().indices.delete(index=settings.ORDER_INDEX)


def refresh_index():
    return get_es_client().indices.refresh(index=settings.ORDER_INDEX)


def add_document(doc_id: str, body: dict):
    return get_es_client().index(index=settings.ORDER_INDEX, id=doc_id, document=body)


def get_document(doc_id: str):
    return get_es_client().get(index=settings.ORDER_INDEX, id=doc_id)


def search_documents(keyword: str | None = None, status: str | None = None):
    must = []
    filters = []

    if keyword:
        must.append(
            {
                "multi_match": {
                    "query": keyword,
                    "fields": ["order_id", "customer_name", "remark"],
                }
            }
        )

    if status:
        filters.append({"term": {"status": status}})

    body = {
        "query": {"bool": {"must": must, "filter": filters}},
        "sort": [{"created_at": {"order": "desc"}}],
    }

    return get_es_client().search(index=settings.ORDER_INDEX, body=body)
