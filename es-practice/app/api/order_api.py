# app/api/es_demo.py
from fastapi import APIRouter
from app.schemas.order_schema import OrderDoc
from app.services.order_service import (
    create_index,
    delete_index,
    refresh_index,
    add_document,
    get_document,
    # search_documents,
)

router = APIRouter(prefix="/es/order", tags=["es-order"])


@router.post("/index")
def api_create_index():
    return create_index()


@router.delete("/index")
def api_delete_index():
    return delete_index()


@router.post("/refresh")
def api_refresh_index():
    return refresh_index()


@router.post("/doc/{doc_id}")
def api_add_document(doc_id: str, payload: OrderDoc):
    return add_document(doc_id, payload.model_dump())


@router.get("/doc/{doc_id}")
def api_get_document(doc_id: str):
    return get_document(doc_id)


@router.get("/search")
def api_search_documents(keyword: str | None = None, status: str | None = None):
    # return search_documents(keyword=keyword, status=status)
    pass
