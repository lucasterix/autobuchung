from fastapi import FastAPI

from .bank_import import router as bank_router

app = FastAPI(title="Autobuchung")
app.include_router(bank_router)


@app.get("/health")
def health():
    return {"ok": True}
