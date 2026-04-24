import logging

from fastapi import FastAPI


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
)

from .bank_import import router as bank_router

app = FastAPI(title="Autobuchung")
app.include_router(bank_router)


@app.get("/health")
def health():
    return {"ok": True}
