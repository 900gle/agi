# src/search_graph/config.py
import os
from dataclasses import dataclass
from typing import Any, Dict

import yaml
from dotenv import load_dotenv


@dataclass
class ESConfig:
    url: str
    verify_certs: bool
    index_name: str
    key_field: str
    query_file: str


@dataclass
class Neo4jConfig:
    uri: str
    user: str
    password: str


@dataclass
class AppConfig:
    log_level: str
    es: ESConfig
    neo4j: Neo4jConfig


def load_config(path: str = "config.yml") -> AppConfig:
    # .env 먼저 로드
    load_dotenv()

    with open(path, encoding="utf-8") as f:
        raw: Dict[str, Any] = yaml.safe_load(f)

    # YAML + ENV 합치기
    es_section = raw.get("elasticsearch", {})
    neo4j_section = raw.get("neo4j", {})

    es_cfg = ESConfig(
        url=os.getenv("ELASTIC_URL", es_section.get("url", "")),
        verify_certs=bool(es_section.get("verify_certs", False)),
        index_name=es_section.get("index_name", ""),
        key_field=es_section.get("key_field", "query_log"),
        query_file=es_section.get("query_file", "./query/searchlog.json"),
    )

    neo_cfg = Neo4jConfig(
        uri=os.getenv("NEO4J_URI", neo4j_section.get("uri", "bolt://localhost:7687")),
        user=os.getenv("NEO4J_USER", neo4j_section.get("user", "neo4j")),
        password=os.getenv(
            "NEO4J_PASSWORD",
            neo4j_section.get("password", "neo4j"),
        ),
    )

    return AppConfig(
        log_level=raw.get("log_level", "INFO"),
        es=es_cfg,
        neo4j=neo_cfg,
    )
