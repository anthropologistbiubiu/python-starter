from fastapi import FastAPI
from app.client.es_client import get_es_client


def create_app() -> FastAPI:
    app = FastAPI()

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/es/ping")
    def es_ping():
        try:
            es = get_es_client()
            ping_result = es.ping()
            info_result = es.info()
            return {
                "connected": ping_result,
                "info": info_result,
            }
        except Exception as e:
            return {
                "connected": False,
                "error": repr(e),
            }

    return app


app = create_app()
