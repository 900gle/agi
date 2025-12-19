import os
from fastapi import FastAPI
from pydantic import BaseModel
from neo4j import GraphDatabase

app = FastAPI()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "2Engussla")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


class ClickPath(BaseModel):
    from_kw: str
    to_kw: str


@app.on_event("shutdown")
def close_driver():
    driver.close()


@app.post("/next")
def create_next(cp: ClickPath):
    """
    ex)
    POST /next
    {
      "from_kw": "치킨",
      "to_kw": "닭고기"
    }
    """
    cypher = """
    MERGE (a:Keyword {name:$from_kw})
    MERGE (b:Keyword {name:$to_kw})
    MERGE (a)-[r:NEXT]->(b)
    ON CREATE SET r.count = 1
    ON MATCH SET r.count = r.count + 1
    RETURN r.count AS count
    """
    with driver.session() as session:
        result = session.run(
            cypher,
            from_kw=cp.from_kw,
            to_kw=cp.to_kw,
        )
        record = result.single()
        return {"from": cp.from_kw, "to": cp.to_kw, "count": record["count"]}
@app.get("/node/{name}")
def get_node(name: str, include_next: bool = True, limit: int = 50):
    """
    키워드 노드 조회 (옵션: NEXT 이웃까지)

    - GET /node/치킨
    - GET /node/치킨?include_next=true&limit=10
    """
    if include_next:
        cypher = """
        MATCH (k:Keyword {name:$name})
        OPTIONAL MATCH (k)-[r:NEXT]->(n:Keyword)
        RETURN
          k.name AS name,
          collect({name: n.name, count: r.count})[0..$limit] AS next
        """
        with driver.session() as session:
            record = session.run(cypher, name=name, limit=limit).single()
            if record is None:
                return {"found": False, "name": name}
            return {
                "found": True,
                "name": record["name"],
                "next": [x for x in (record["next"] or []) if x.get("name") is not None],
            }

    cypher = """
    MATCH (k:Keyword {name:$name})
    RETURN k.name AS name
    """
    with driver.session() as session:
        record = session.run(cypher, name=name).single()
        if record is None:
            return {"found": False, "name": name}
        return {"found": True, "name": record["name"]}


@app.get("/nodes")
def list_nodes(limit: int = 100):
    """
    전체 Keyword 노드 목록(일부) 조회

    - GET /nodes?limit=100
    """
    cypher = """
    MATCH (k:Keyword)
    RETURN k.name AS name
    ORDER BY name
    LIMIT $limit
    """
    with driver.session() as session:
        rows = session.run(cypher, limit=limit).data()
        return {"count": len(rows), "nodes": [r["name"] for r in rows]}
