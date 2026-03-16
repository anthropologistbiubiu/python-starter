

from elasticsearch import Elasticsearch
import logging

# 启用日志输出
logging.basicConfig(level=logging.DEBUG)

es = Elasticsearch(
    ["http://106.52.208.15:9200"],
    headers={"Accept": "application/vnd.elasticsearch+json;compatible-with=8"}
)

print(es.info())
