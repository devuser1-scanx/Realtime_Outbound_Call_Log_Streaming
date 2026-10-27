import json
import logging
import sys


class JsonFormatter(logging.Formatter):
    def format(self, record):
        out = {
            "severity": record.levelname,
            "message": record.getMessage(),
        }
        for key in ("call_log_id", "event_id"):
            if hasattr(record, key):
                out[key] = getattr(record, key)
        if record.exc_info:
            out["exception"] = self.formatException(record.exc_info)
        return json.dumps(out)


class _FlushingHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()


def setup_logging(level="INFO"):
    handler = _FlushingHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
