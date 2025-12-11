from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "2Engussla"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

# ----------------------------------------------------
# 1) 기존 데이터 모두 삭제 (테스트 환경용)
# ----------------------------------------------------
def clear_all(tx):
    tx.run("MATCH (n) DETACH DELETE n")


# ----------------------------------------------------
# 2) 키워드 체인 생성
# ----------------------------------------------------
def create_keyword_chain(tx):
    query = """
    CREATE
      (k1:Keyword {name: "치킨"})-
      [:NEXT]->(k2:Keyword {name: "닭고기"})-
      [:NEXT]->(k3:Keyword {name: "조리식품"})-
      [:NEXT]->(k4:Keyword {name: "튀김"})-
      [:NEXT]->(k5:Keyword {name: "냉동식품"});
    """
    tx.run(query)


# ----------------------------------------------------
# 3) 잘 들어갔는지 조회
# ----------------------------------------------------
def print_keywords(tx):
    result = tx.run("MATCH (k:Keyword) RETURN k.name AS name")
    for record in result:
        print(record["name"])


# ----------------------------------------------------
# 메인 실행
# ----------------------------------------------------
if __name__ == "__main__":
    with driver.session() as session:

        print("기존 데이터 삭제...")
        session.execute_write(clear_all)

        print("그래프 생성...")
        session.execute_write(create_keyword_chain)

        print("저장된 키워드 확인:")
        session.execute_read(print_keywords)

    driver.close()
