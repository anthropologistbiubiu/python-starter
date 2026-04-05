from app.repositories.base_repository import BaseRepository

ORDER_INDEX = {
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


class OrderRepository(BaseRepository):
    def __init__(self):
        super().__init__(ORDER_INDEX)

    def create_order_index(self, order_index):
        return self.create_index(order_index)

    def delete_order_index(self, order_index):
        return self.delete_index(order_index)

    def add_order(self, order_id: str, order_data: dict):
        return self.index_doc(order_id, order_data)

    def get_order(self, order_id: str) -> dict:
        return self.get_doc(doc_id=order_id)
