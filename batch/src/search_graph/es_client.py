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

    def aggregate_user_pcid(
        self,
        index_name: str,
        gte: str,
        lte: str,
        size: int = 10000,
    ):
        """
        특정 기간 동안 user_pcid 기준으로 terms 집계를 수행하고
        USER_PCID buckets 를 반환한다.
        """
        logger.info(
            "user_pcid 집계 실행: index=%s, gte=%s, lte=%s, size=%d",
            index_name,
            gte,
            lte,
            size,
        )

        body = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "query_log.created_date_time": {
                                    "gte": gte,
                                    "lte": lte,
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "USER_PCID": {
                    "terms": {
                        "field": "query_log.user_pcid.keyword",
                        "size": size,
                    }
                }
            },
        }

        logger.debug("user_pcid 집계 body: %s", body)

        response = self.client.search(
            index=index_name,
            body=body,
        )

        buckets = (
            response.get("aggregations", {})
            .get("USER_PCID", {})
            .get("buckets", [])
        )

        logger.info("user_pcid 집계 결과 bucket 개수: %d", len(buckets))
        return buckets