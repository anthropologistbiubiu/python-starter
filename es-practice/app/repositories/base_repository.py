# app/repositories/base_repository.py
from app.client.es_client import get_es_client


class BaseRepository:
    def __init__(self, index_name: str):
        self.client = get_es_client()
        self.index_name = index_name

    def create_index(self, body: dict):
        if not self.client.indices.exists(index=self.index_name):
            return self.client.indices.create(index=self.index_name, body=body)
        return {"acknowledged": True, "message": "index already exists"}

    def delete_index(self):
        if self.client.indices.exists(index=self.index_name):
            return self.client.indices.delete(index=self.index_name)
        return {"acknowledged": True, "message": "index not exists"}

    def index_doc(self, doc_id: str, body: dict):
        return self.client.index(index=self.index_name, id=doc_id, document=body)

    def get_doc(self, doc_id: str):
        return self.client.get(index=self.index_name, id=doc_id)

    def delete_doc(self, doc_id: str):
        return self.client.delete(index=self.index_name, id=doc_id)

    def search(self, body: dict):
        return self.client.search(index=self.index_name, body=body)
