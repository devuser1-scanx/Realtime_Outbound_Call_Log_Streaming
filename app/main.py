import logging
import threading
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .config import load_config
from .db import Database
from .logging_setup import setup_logging
from .loop import StreamLoop
from .publisher import Publisher

HEALTH_MAX_AGE_SECONDS = 120

log = logging.getLogger(__name__)
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

_state = {"loop": None, "db": None, "publisher": None}


@app.on_event("startup")
def startup():
    cfg = load_config()  # fail fast on missing vars
    setup_logging(cfg.log_level)
    db = Database(cfg)
    db.ensure_table()
    publisher = Publisher(cfg.project_id, cfg.topic)
    loop = StreamLoop(cfg, db, publisher)
    # Startup counts as the first success so /healthz is not 503 before the first iteration.
    loop.last_success = datetime.now(timezone.utc)
    _state.update(loop=loop, db=db, publisher=publisher)
    threading.Thread(target=loop.run_forever, name="stream-loop", daemon=True).start()
    log.info("started")


@app.on_event("shutdown")
def shutdown():
    if _state["loop"]:
        _state["loop"].stop()
    if _state["publisher"]:
        _state["publisher"].close()
    if _state["db"]:
        _state["db"].close()


@app.get("/healthz")
def healthz():
    loop = _state["loop"]
    last = loop.last_success if loop else None
    age = (datetime.now(timezone.utc) - last).total_seconds() if last else None
    if age is None or age > HEALTH_MAX_AGE_SECONDS:
        return JSONResponse(
            {"ok": False, "last_success": last.isoformat() if last else None},
            status_code=503,
        )
    return {"ok": True, "last_success": last.isoformat(), "pending_estimate": loop.pending_estimate}
