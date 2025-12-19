# src/search_graph/search_log_processor.py
import logging
from collections import Counter
from typing import Any, Dict, Tuple, List

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
        fail_pair_csv_path: str = "./result/fail_pair_candidates.csv",
    ):
        self.es = es
        self.neo = neo
        self.index_name = index_name
        self.query_file = query_file
        self.key_field = key_field
        self.fail_pair_csv_path = fail_pair_csv_path

    # ---------------------------------------------------------
    # 1) 기존 전체 로그 처리 (query_file 기반)
    #    → 내부 로직은 process_hits()를 재사용하도록 정리
    # ---------------------------------------------------------
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

        hits = response.get("hits", {}).get("hits", [])

        # 공통 로직 재사용
        fail_pairs = self.process_hits(hits)

        # FAIL_NEXT 후보 CSV 출력
        self._write_fail_pairs_csv(fail_pairs)

        logger.info("검색 로그 처리 완료")

    # ---------------------------------------------------------
    # 2) FAIL_NEXT 후보 CSV 저장
    # ---------------------------------------------------------
    def _write_fail_pairs_csv(self, fail_pairs: Counter[Tuple[str, str]]):
        import os

        # 결과 디렉토리 없으면 만들기
        os.makedirs(os.path.dirname(self.fail_pair_csv_path), exist_ok=True)

        with open(self.fail_pair_csv_path, "w", encoding="utf-8") as f:
            f.write("A,B,pair_count\n")
            for (a, b), c in fail_pairs.most_common():
                f.write(f"{a},{b},{c}\n")

        logger.info(
            "FAIL_NEXT 후보 CSV 저장: %s (rows=%d)",
            self.fail_pair_csv_path,
            len(fail_pairs),
        )

    # ---------------------------------------------------------
    # 3) user_pcid 집계 → CSV 저장
    # ---------------------------------------------------------
    def export_user_pcid(
        self,
        gte: str,
        lte: str,
        size: int = 10,
        output_path: str = "./result/user_pcid_list.csv",
    ):
        """
        ES에서 user_pcid 집계해서 CSV로 저장.
        컬럼: user_pcid,doc_count
        """
        logger.info(
            "user_pcid 리스트 export 시작: index=%s, gte=%s, lte=%s, size=%d, output=%s",
            self.index_name,
            gte,
            lte,
            size,
            output_path,
        )

        buckets = self.es.aggregate_user_pcid(
            index_name=self.index_name,
            gte=gte,
            lte=lte,
            size=size,
        )

        import os

        # ./result 디렉토리 없으면 생성
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write("user_pcid,doc_count\n")
            for bucket in buckets:
                pcid = bucket.get("key")
                doc_count = bucket.get("doc_count", 0)
                f.write(f"{pcid},{doc_count}\n")

        logger.info(
            "user_pcid 리스트 export 완료: buckets=%d, file=%s",
            len(buckets),
            output_path,
        )

    # ---------------------------------------------------------
    # 4) 특정 PCID에 대한 로그 조회 (기간 + 정렬)
    # ---------------------------------------------------------
    def fetch_hits_by_pcid(
        self,
        pcid: str,
        gte: str,
        lte: str,
        size: int = 10000,
    ) -> List[Dict[str, Any]]:
        """
        특정 pcid에 대한 로그를 시간순으로 조회.
        기존 query file 방식이 아닌, processor 내부에서 쿼리를 직접 생성한다.
        """
        logger.info("PCID별 로그 조회 시작: pcid=%s", pcid)

        body = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"query_log.user_pcid.keyword": pcid}},
                    ],
                    "filter": [
                        {
                            "range": {
                                "query_log.created_date_time": {
                                    "gte": gte,
                                    "lte": lte,
                                }
                            }
                        }
                    ],
                }
            },
            "sort": [{"query_log.created_date_time": {"order": "asc"}}],
            "size": size,
        }

        response = self.es.client.search(
            index=self.index_name,
            body=body,
        )

        hits = response.get("hits", {}).get("hits", [])
        logger.info("PCID=%s hits 개수: %d", pcid, len(hits))

        return hits

    # ---------------------------------------------------------
    # 5) hits 배열 하나를 받아서 그래프/FAIL_NEXT 처리
    #    (공통 처리 로직)
    # ---------------------------------------------------------
    def process_hits(self, hits: List[Dict[str, Any]]) -> Counter[Tuple[str, str]]:
        prev_key = ""
        prev_date = ""
        pending_fail_A = ""
        fail_pairs: Counter[Tuple[str, str]] = Counter()

        for hit in hits:
            source: Dict[str, Any] = hit.get("_source", {})
            query_log: Dict[str, Any] = source.get(self.key_field, {})

            # A/B는 일단 search_query 기준 (원하면 input_query로 바꿀 수 있음)
            key = str(query_log.get("search_query") or "").strip()
            created_dt = str(query_log.get("created_date_time") or "")
            created_date = created_dt[:10] if created_dt else ""

            # TODO: 실제 운영에서는 result_count == 0 인 경우만 실패로 볼 예정
            # result_count = query_log.get("result_count", None)
            # is_fail = (result_count == 0)
            is_fail = True  # 현재는 MVP용: 일단 모두 실패로 간주

            if not key or not created_date:
                continue

            # 날짜가 바뀌면: 체인/실패 대기 상태 리셋
            if prev_date and created_date != prev_date:
                prev_key = ""
                pending_fail_A = ""

            # 1) Keyword 노드 MERGE
            self.neo.merge_keyword(key)

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

        return fail_pairs

    # ---------------------------------------------------------
    # 6) 단일 PCID 처리: ES 조회 → process_hits
    # ---------------------------------------------------------
    def process_pcid(
        self,
        pcid: str,
        gte: str,
        lte: str,
        size: int = 10000,
    ) -> Counter[Tuple[str, str]]:
        """
        하나의 PCID에 대해 검색 시퀀스를 Neo4j에 반영하고,
        fail_next 후보를 Counter로 반환한다.
        """
        hits = self.fetch_hits_by_pcid(pcid, gte, lte, size)
        fail_pairs = self.process_hits(hits)

        logger.info(
            "PCID 처리 완료: pcid=%s, fail_pairs=%d",
            pcid,
            len(fail_pairs),
        )
        return fail_pairs

    # ---------------------------------------------------------
    # 7) user_pcid 리스트 전체 처리
    #    - CSV(user_pcid_list.csv)를 읽어서
    #      각 PCID에 대해 process_pcid 수행
    #    - 최종 FAIL_NEXT 후보를 CSV로 저장
    # ---------------------------------------------------------
    def process_all_pcids(
        self,
        pcid_list_file: str,
        gte: str,
        lte: str,
        size: int = 10000,
    ) -> Counter[Tuple[str, str]]:
        """
        CSV에 담긴 user_pcid 리스트를 읽어서
        각 PCID에 대해 process_pcid() 수행 후,
        FAIL_NEXT 후보를 합산하여 CSV로 저장한다.
        """
        import csv

        with open(pcid_list_file, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            pcids = [row["user_pcid"] for row in reader]

        logger.info("PCID 전체 처리 시작: %d명", len(pcids))

        total_fail_pairs: Counter[Tuple[str, str]] = Counter()

        for pcid in pcids:
            fail_pairs = self.process_pcid(pcid, gte, lte, size)
            total_fail_pairs.update(fail_pairs)

        logger.info(
            "PCID 전체 처리 완료: 총 fail_pairs=%d",
            len(total_fail_pairs),
        )

        # 전체 PCID 기반 FAIL_NEXT 후보를 CSV로 저장
        self._write_fail_pairs_csv(total_fail_pairs)

        return total_fail_pairs
