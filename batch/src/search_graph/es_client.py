# src/search_graph/es_client.py
import json
import logging
import warnings

import urllib3
from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings(
    "ignore",
    message="Connecting to .* using SSL with verify_certs=False is insecure.",
)


class ElasticsearchService:
    def __init__(self, url: str, verify_certs: bool = False):
        self.client = Elasticsearch(url, verify_certs=verify_certs)
        logger.info("Elasticsearch 클라이언트 생성: %s", url)

    def search_with_query_file(self, index_name: str, query_file: str):
        logger.info("ES 검색 실행: index=%s, query_file=%s", index_name, query_file)

        with open(query_file, encoding="utf-8") as f:
            query_source = json.load(f)

        body = {
            "query": query_source["query"],
            "sort": query_source["sort"],
        }
        size = query_source.get("size", 10000)

        logger.debug("ES query body: %s", body)
        response = self.client.search(
            index=index_name,
            body=body,
            size=size,
        )
        hits = response.get("hits", {}).get("hits", [])
        logger.info("ES 결과 hits 개수: %d", len(hits))
        return response
