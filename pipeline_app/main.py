"""CVM Pipeline — FastAPI приложение (новая архитектура, 9 шагов + шаг 0).

Запуск:  python -m pipeline_app.main      (порт 8503)
Старый Streamlit (app.py, порт 8502) не затрагивается.
"""
from __future__ import annotations

from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from pipeline_app import settings
from pipeline_app.core import datasource
from pipeline_app.routers import (
    step0, step1, step2, step3, step4, step5, step6, step7, step8, step9,
)

app = FastAPI(title="CVM Pipeline")
app.mount("/static", StaticFiles(directory=str(Path(__file__).resolve().parent / "static")), name="static")

for r in (step0, step1, step2, step3, step4, step5, step6, step7, step8, step9):
    app.include_router(r.router)


@app.get("/")
def root():
    return RedirectResponse("/step0")


@app.post("/refresh")
def refresh():
    datasource.clear_cache()
    return RedirectResponse("/step0", status_code=303)


if __name__ == "__main__":
    uvicorn.run("pipeline_app.main:app", host="127.0.0.1", port=settings.PORT, reload=False)
