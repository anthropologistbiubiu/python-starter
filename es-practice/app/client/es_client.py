

from app.config.settings import settings
from elasticsearch import Elasticsearch


# app/client/es_client.py


def get_es_client() -> Elasticsearch:
    return Elasticsearch(
        hosts=[settings.ES_HOST],
        # basic_auth=(settings.ES_USER, settings.ES_PASSWORD),
        # request_timeout=settings.ES_TIMEOUT,
        # verify_certs=False,
    )
