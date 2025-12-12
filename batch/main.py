# main.py
from es_client import ElasticsearchService
from neo4j_client import Neo4jService
from search_log_processor import SearchLogProcessor


def main():
    # -------- 설정값 --------
    KEY_FIELD = "query_log"
    INDEX_NAME = "home-search-query-log"
    QUERY_FILE = "./query/searchlog.json"
    RESULT_FILE = "./result/log_result.txt"  # 지금은 안 쓰고 있지만 남겨둠

    ES_URL = "https://elastic:elastic1!@searchlog-es.homeplus.co.kr:443/"
    NEO4J_URI = "bolt://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "2Engussla"
    # ------------------------

    # 클라이언트 생성
    es_service = ElasticsearchService(ES_URL, verify_certs=False)
    neo_service = Neo4jService(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

    # 로그 → 그래프 적재
    processor = SearchLogProcessor(
        es=es_service,
        neo=neo_service,
        index_name=INDEX_NAME,
        query_file=QUERY_FILE,
        key_field=KEY_FIELD,
    )

    # 필요하면 파일 작업도 여기에서
    # with open(RESULT_FILE, "w", encoding="utf-8") as f:
    #     pass  # 향후 결과 쓰고 싶으면 여기서 처리

    processor.process()

    # Neo4j 테스트 및 예시 출력
    neo_service.test_connection()
    print(neo_service.get_next_list("테라"))

    neo_service.close()


if __name__ == "__main__":
    main()
