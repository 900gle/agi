# src/search_graph/logging_config.py
import logging


def setup_logging(level: str = "INFO") -> None:
    level_obj = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=level_obj,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
