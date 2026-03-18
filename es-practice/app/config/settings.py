# app/config/settings.py
class Settings:
    ES_HOST = "http://121.41.22.84:9200"
    # ES_USER = "elastic"
    # ES_PASSWORD = "changeme"
    ES_TIMEOUT = 30

    ORDER_INDEX = "orders_v1"
    LOG_INDEX = "operation_logs_v1"


settings = Settings()
