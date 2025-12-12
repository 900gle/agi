# src/search_graph/search_log_processor.py
import logging
from typing import Any, Dict
from .es_client import ElasticsearchService
from .neo4j_client import Neo4jService
from collections import Counter
from typing import Any, Dict, Tuple

logger = logging.getLogger(__name__)

class SearchLogProcessor:
    def __init__(
        self,
        es: ElasticsearchService,
        neo: Neo4jService,
        index_name: str,
        query_file: str,
        key_field: str = "query_log",
        fail_pair_csv_path: str = "./result/fail_pair_candidates.csv",
    ):
        self.es = es
        self.neo = neo
        self.index_name = index_name
        self.query_file = query_file
        self.key_field = key_field
        self.fail_pair_csv_path = fail_pair_csv_path

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

        prev_key = ""
        prev_date = ""
        pending_fail_A = ""  # 직전 실패(0) 검색어 A
        fail_pairs: Counter[Tuple[str, str]] = Counter()

        for hit in response.get("hits", {}).get("hits", []):
            source: Dict[str, Any] = hit.get("_source", {})
            query_log: Dict[str, Any] = source.get(self.key_field, {})

            # A/B는 일단 search_query 기준 (원하면 input_query로 바꿀 수 있음)
            key = str(query_log.get("search_query") or "").strip()
            created_dt = str(query_log.get("created_date_time") or "")
            created_date = created_dt[:10] if created_dt else ""

            # 실패 여부 (없으면 None -> 실패로 보지 않음)
            result_count = query_log.get("result_count", None)
            # is_fail = (result_count == 0)
            is_fail = True

            if not key or not created_date:
                continue

            # 날짜가 바뀌면: 체인/실패 대기 상태 리셋
            if prev_date and created_date != prev_date:
                prev_key = ""
                pending_fail_A = ""

            # 1) Keyword 노드 생성 (중복은 neo4j에서 MERGE로 처리하는게 이상적)
            #    지금 create_keyword가 CREATE라면 중복 생성될 수 있으니,
            #    가능하면 neo.create_keyword를 MERGE로 바꾸는 걸 추천.
            self.neo.merge_keyword(key)  # 아래 Neo4jService에 추가할 메서드(권장)

            # 2) 인접 전이 NEXT (같은 날짜 내에서만)
            if prev_key and created_date == prev_date and prev_key != key:
                self.neo.create_next_relation(prev_key, key)

            # 3) 실패쌍 FAIL_NEXT 후보: "A가 실패였고 다음 검색어가 B"
            if pending_fail_A and pending_fail_A != key:
                # Neo4j에 FAIL_NEXT 누적
                self.neo.create_fail_next_relation(pending_fail_A, key, created_dt)
                # CSV 집계 누적
                fail_pairs[(pending_fail_A, key)] += 1
                # 한 번 매칭하면 대기 해제 (A 다음 검색 1개만 붙이는 MVP)
                pending_fail_A = ""

            # 4) 현재 검색이 실패면 A로 대기
            if is_fail:
                pending_fail_A = key
            else:
                # 성공 검색이면 A 대기 해제(원하면 유지해도 되지만 MVP에선 해제 추천)
                pending_fail_A = ""

            # 5) prev 갱신 (인접 전이의 핵심)
            prev_key = key
            prev_date = created_date

        # 6) fail pair CSV 출력
        self._write_fail_pairs_csv(fail_pairs)

        logger.info("검색 로그 처리 완료")

    def _write_fail_pairs_csv(self, fail_pairs: Counter[Tuple[str, str]]):
        # 결과 디렉토리 없으면 만들기
        import os
        os.makedirs(os.path.dirname(self.fail_pair_csv_path), exist_ok=True)

        with open(self.fail_pair_csv_path, "w", encoding="utf-8") as f:
            f.write("A,B,pair_count\n")
            for (a, b), c in fail_pairs.most_common():
                f.write(f"{a},{b},{c}\n")
        logger.info("FAIL_NEXT 후보 CSV 저장: %s (rows=%d)", self.fail_pair_csv_path, len(fail_pairs))
