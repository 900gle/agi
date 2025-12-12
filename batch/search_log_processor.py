# src/search_graph/search_log_processor.py
import logging
from typing import Any, Dict

from .es_client import ElasticsearchService
from .neo4j_client import Neo4jService

logger = logging.getLogger(__name__)


class SearchLogProcessor:
    def __init__(
        self,
        es: ElasticsearchService,
        neo: Neo4jService,
        index_name: str,
        query_file: str,
        key_field: str = "query_log",
    ):
        self.es = es
        self.neo = neo
        self.index_name = index_name
        self.query_file = query_file
        self.key_field = key_field

    def process(self):
        logger.info(
            "검색 로그 처리 시작: index=%s, query_file=%s, key_field=%s",
            self.index_name,
            self.query_file,
            self.key_field,
        )

        response = self.es.search_with_query_file(
            index_name=self.index_name,
            query_file=self.query_file,
        )

        from_key = ""
        date_key = ""

        for hit in response.get("hits", {}).get("hits", []):
            source: Dict[str, Any] = hit.get("_source", {})
            query_log: Dict[str, Any] = source.get(self.key_field, {})

            key = str(query_log.get("search_query") or "").strip()
            created_dt = query_log.get("created_date_time", "")
            created_date = created_dt[:10] if created_dt else ""

            logger.debug("data_key: %s == %s, key=%s", date_key, created_date, key)

            if not key or not created_date:
                logger.debug("search_query 또는 created_date_time 누락, skip")
                continue

            if date_key == created_date:
                if from_key:
                    self.neo.create_next_relation(from_key, key)
            else:
                self.neo.create_keyword(key)
                from_key = key
                date_key = created_date

        logger.info("검색 로그 처리 완료")
