from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "2Engussla"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))


def test_connection():
    with driver.session() as session:
        result = session.run("RETURN 'Connected!' AS msg")
        print(result.single()["msg"])


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
    test_connection()

    create_keyword("치킨")
    create_keyword("닭고기")
    create_keyword("튀김")

    create_next_relation("치킨", "닭고기")
    create_next_relation("치킨", "튀김")
    create_next_relation("치킨", "닭고기")  # count 증가

    print(get_next_list("치킨"))

    driver.close()
