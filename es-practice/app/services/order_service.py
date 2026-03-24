# app/services/es_demo_service.py
from app.config.settings import settings
from app.client.es_client import get_es_client
from app.repositories.order_repository import OrderRepository


order_repo = OrderRepository()


def create_index():
    return order_repo.create_order_index()


def delete_index():
    return order_repo.delete_order_index()


def refresh_index():
    return order_repo().indices.refresh()


def add_document(doc_id: str, body: dict):
    return order_repo.add_order(id=doc_id, document=body)


def get_document(doc_id: str):
    return order_repo.get_order(id=doc_id)


def search_documents(keyword: str | None = None, status: str | None = None):
    """
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

    return order_repo().search(body=body)

    """
