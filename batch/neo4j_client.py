# src/search_graph/neo4j_client.py
import logging
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)


class Neo4jService:
    def __init__(self, uri: str, user: str, password: str):
        logger.info("Neo4j 드라이버 초기화: %s", uri)
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        logger.info("Neo4j 드라이버 종료")
        self._driver.close()

    def _run(self, query: str, **params):
        logger.debug("Cypher 실행: %s, params=%s", query, params)
        with self._driver.session() as session:
            return session.run(query, **params)

    def test_connection(self):
        result = self._run("RETURN 'Connected!' AS msg")
        logger.info("Neo4j 연결 상태: %s", result.single()["msg"])

        node_count = self._run("MATCH (n) RETURN count(n) AS c").single()["c"]
        rel_count = self._run("MATCH ()-[r:NEXT]-() RETURN count(r) AS c").single()["c"]
        logger.info("노드 개수: %d, NEXT 관계 개수: %d", node_count, rel_count)

    def clear_all(self):
        logger.warning("모든 노드/관계를 삭제합니다.")
        self._run("MATCH (n) DETACH DELETE n")

    def create_keyword(self, name: str):
        logger.debug("키워드 노드 생성: %s", name)
        query = "CREATE (k:Keyword {name:$name}) RETURN k"
        return self._run(query, name=name).single()

    def create_next_relation(self, from_kw: str, to_kw: str):
        logger.debug("NEXT 관계 생성/증가: %s -> %s", from_kw, to_kw)
        query = """
        MATCH (a:Keyword {name:$from_kw}), (b:Keyword {name:$to_kw})
        MERGE (a)-[r:NEXT]->(b)
        ON CREATE SET r.count = 1
        ON MATCH SET r.count = r.count + 1
        RETURN r
        """
        return self._run(query, from_kw=from_kw, to_kw=to_kw).single()

    def get_next_list(self, name: str):
        logger.debug("NEXT 리스트 조회: %s", name)
        query = """
        MATCH (a:Keyword {name:$name})-[r:NEXT]->(b)
        RETURN b.name AS next, r.count AS count
        ORDER BY count DESC
        """
        return self._run(query, name=name).data()
