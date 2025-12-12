# src/search_graph/cli.py
import argparse
import logging

from .config import load_config
from .logging_config import setup_logging
from .es_client import ElasticsearchService
from .neo4j_client import Neo4jService
from .search_log_processor import SearchLogProcessor


logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="ES 검색로그를 Neo4j 그래프로 적재")
    parser.add_argument(
        "-c",
        "--config",
        default="config.yaml",
        help="설정 파일 경로 (기본값: config.yaml)",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    setup_logging(cfg.log_level)

    logger.info("애플리케이션 시작")

    es_service = ElasticsearchService(cfg.es.url, verify_certs=cfg.es.verify_certs)
    neo_service = Neo4jService(cfg.neo4j.uri, cfg.neo4j.user, cfg.neo4j.password)

    processor = SearchLogProcessor(
        es=es_service,
        neo=neo_service,
        index_name=cfg.es.index_name,
        query_file=cfg.es.query_file,
        key_field=cfg.es.key_field,
    )

    try:
        processor.process()
        neo_service.test_connection()
        top_next = neo_service.get_next_list("테라")
        logger.info("테라 NEXT 리스트: %s", top_next)
    finally:
        neo_service.close()
        logger.info("애플리케이션 종료")
