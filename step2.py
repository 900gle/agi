import json
import urllib3
from elasticsearch import Elasticsearch
import warnings
from neo4j import GraphDatabase

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Connecting to .* using SSL with verify_certs=False is insecure.")


URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "2Engussla"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

# 전체 로그 가져오기
def extrect():
    with open(QUERY_FILE, encoding='utf-8') as query_file:
        query_source = json.load(query_file)
        response = client.search(
            index=INDEX_NAME,
            body={
                "query": query_source["query"],
                "sort": query_source["sort"]
            },
            size=query_source.get("size", 10000)
        )

    from_key = ""
    date_key = ""
    for hit in response.get("hits", {}).get("hits", []):
        source = hit.get("_source", {})
        query_log = source.get(KEY_FIELD, {})


        key = str(query_log.get("search_query")).strip()

        print("data_key : " + date_key + " == " + query_log.get("created_date_time")[:10])

        if date_key == query_log.get("created_date_time")[:10]:
            create_next_relation(from_key, key)
        else:
            create_keyword(key)
            from_key = key
        date_key = query_log.get("created_date_time")[:10]


def test_connection():
    with driver.session() as session:
        result = session.run("RETURN 'Connected!' AS msg")

        print(session.run("MATCH (n) RETURN count(n) AS c").single()["c"])
        print(session.run("MATCH ()-[r:NEXT]-() RETURN count(r) AS c").single()["c"])
        for rec in session.run("""
                MATCH (a:Keyword)-[r:NEXT]->(b:Keyword)
                RETURN a.name AS from, r.count AS count, b.name AS to
                ORDER BY count DESC
                LIMIT 20
            """):
            print(rec["from"], rec["count"], rec["to"])


        # session.execute_write(clear_all)
        print(result.single()["msg"])

def clear_all(tx):
    tx.run("MATCH (n) DETACH DELETE n")

def create_keyword(name):
    with driver.session() as session:
        query = "CREATE (k:Keyword {name:$name}) RETURN k"
        return session.run(query, name=name).single()


def create_next_relation(from_kw, to_kw):
    query = """
    MATCH (a:Keyword {name:$from_kw}), (b:Keyword {name:$to_kw})
    MERGE (a)-[r:NEXT]->(b)
    ON CREATE SET r.count = 1
    ON MATCH SET r.count = r.count + 1
    RETURN r
    """
    with driver.session() as session:
        return session.run(query, from_kw=from_kw, to_kw=to_kw).single()


def get_next_list(name):
    query = """
    MATCH (a:Keyword {name:$name})-[r:NEXT]->(b)
    RETURN b.name AS next, r.count AS count
    ORDER BY count DESC
    """
    with driver.session() as session:
        return session.run(query, name=name).data()


# 실습 예시
if __name__ == "__main__":
    KEY_FIELD = "query_log"
    INDEX_NAME  = "home-search-query-log"
    QUERY_FILE  = "./query/searchlog.json"
    RESULT_FILE = "./result/log_result.txt"
    client = Elasticsearch(
        "https://elastic:elastic1!@searchlog-es.homeplus.co.kr:443/",
        verify_certs=False
    )
    with open(RESULT_FILE, 'w', encoding='utf-8') as f:
        extrect()

    test_connection()
    print(get_next_list("테라"))

    driver.close()
