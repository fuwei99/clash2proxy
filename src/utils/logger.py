from __future__ import annotations

import logging


class _CompatLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return msg, kwargs

    def success(self, msg, *args, **kwargs):
        self.info(msg, *args, **kwargs)


def get_logger(name: str) -> _CompatLogger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    return _CompatLogger(logging.getLogger(name), {})

