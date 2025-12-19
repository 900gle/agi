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

    # 👉 실행 모드: 기존 process + 새 기능 2개
    parser.add_argument(
        "mode",
        nargs="?",
        default="process",
        choices=["process", "export_pcid", "process_all_pcids"],
        help="실행 모드 선택: process / export_pcid / process_all_pcids (기본값: process)",
    )

    parser.add_argument(
        "-c",
        "--config",
        default="config.yml",
        help="설정 파일 경로 (기본값: config.yml)",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    setup_logging(cfg.log_level)

    logger.info("애플리케이션 시작 (mode=%s)", args.mode)

    es_service = ElasticsearchService(cfg.es.url, verify_certs=cfg.es.verify_certs)
    neo_service = Neo4jService(cfg.neo4j.uri, cfg.neo4j.user, cfg.neo4j.password)

    # ⚠️ fail_pair_csv_path 는 기본값 쓰게 두고, 기존처럼 세팅
    processor = SearchLogProcessor(
        es=es_service,
        neo=neo_service,
        index_name=cfg.es.index_name,
        query_file=cfg.es.query_file,
        key_field=cfg.es.key_field,
    )

    try:
        # -------------------------------
        # 1) 기존 전체 로그 처리
        # -------------------------------
        if args.mode == "process":
            processor.process()
            neo_service.test_connection()
            top_next = neo_service.get_next_list("테라")
            logger.info("테라 NEXT 리스트: %s", top_next)

        # -------------------------------
        # 2) PCID 집계 → CSV 생성
        # -------------------------------
        elif args.mode == "export_pcid":
            # 🔹 일단은 하드코딩(테스트용)
            gte = "2024-12-01T00:00:00.000"
            lte = "2025-01-01T00:00:00.000"
            size = 10
            output_path = "./result/user_pcid_list.csv"

            logger.info(
                "[export_pcid] gte=%s, lte=%s, size=%d, output=%s",
                gte, lte, size, output_path
            )

            processor.export_user_pcid(
                gte=gte,
                lte=lte,
                size=size,
                output_path=output_path,
            )

            logger.info("[export_pcid] 완료")

        # -------------------------------
        # 3) PCID 리스트 기반 전체 처리
        # -------------------------------
        elif args.mode == "process_all_pcids":
            # 🔹 이것도 일단 기본값(테스트용)
            pcid_list_file = "./result/user_pcid_list.csv"
            gte = "2024-12-01T00:00:00.000"
            lte = "2025-01-01T00:00:00.000"
            size = 10000

            logger.info(
                "[process_all_pcids] pcid_list_file=%s, gte=%s, lte=%s, size=%d",
                pcid_list_file, gte, lte, size
            )

            processor.process_all_pcids(
                pcid_list_file=pcid_list_file,
                gte=gte,
                lte=lte,
                size=size,
            )

            logger.info("[process_all_pcids] 완료")

    finally:
        neo_service.close()
        logger.info("애플리케이션 종료")

if __name__ == "__main__":
    main()
