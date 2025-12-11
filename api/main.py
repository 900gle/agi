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
