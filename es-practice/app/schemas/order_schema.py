from pydantic import BaseModel
from typing import Optional


class OrderDoc(BaseModel):
    order_id: str
    customer_name: str
    status: str
    country: str
    amount: float
    remark: Optional[str] = None
    created_at: str
