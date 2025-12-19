# Neo4j + FastAPI API (Docker Compose)

이 프로젝트는 Neo4j 그래프 DB와 FastAPI 기반 API 서버를 Docker Compose로 실행하기 위한 템플릿입니다.  
검색 키워드 간 이동 흐름을 `NEXT` 관계로 저장하고 count를 증가시키는 기능을 제공합니다.

---

## 📁 프로젝트 구조

```
.
├── README.md
├── __init__.py
├── api
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
├── batch
│   ├── __init__.py
│   ├── cli.py
│   ├── config.yml
│   ├── es_client.py
│   ├── main.py
│   ├── neo4j_client.py
│   ├── pyproject.toml
│   ├── search_log_processor.py
│   └── src
│       ├── __init__.py
│       ├── search_graph
│       │   ├── __init__.py
│       │   ├── __pycache__
│       │   ├── cli.py
│       │   ├── config.py
│       │   ├── es_client.py
│       │   ├── logging_config.py
│       │   ├── main.py
│       │   ├── neo4j_client.py
│       │   └── search_log_processor.py
│       └── search_graph.egg-info
│           ├── PKG-INFO
│           ├── SOURCES.txt
│           ├── dependency_links.txt
│           ├── entry_points.txt
│           ├── requires.txt
│           └── top_level.txt
├── config.py
├── config.yml
├── docker-compose-full.yml
├── docker-compose.yml
├── logging_config.py
├── neo4j
│   ├── import
│   └── plugins
├── query
│   └── searchlog.json
├── result
│   └── log_result.txt
├── start.py
├── step1.py
├── step2.py
└── step3.py                
```

---

## 🐳 docker-compose.yml

```yaml
version: "3.8"

services:
  neo4j:
    image: neo4j:5.12
    container_name: neo4j
    ports:
      - "7474:7474"
      - "7687:7687"
    environment:
      NEO4J_AUTH: "neo4j/2Engussla"
    volumes:
      - neo4j_data:/data
      - neo4j_logs:/logs
    restart: unless-stopped

  api:
    build: ./api
    container_name: neo4j-api
    depends_on:
      - neo4j
    environment:
      NEO4J_URI: bolt://neo4j:7687
      NEO4J_USER: neo4j
      NEO4J_PASSWORD: 2Engussla
    ports:
      - "8000:8000"
    restart: unless-stopped

volumes:
  neo4j_data:
  neo4j_logs:
  ```

 FastAPI   

 Dockerfile  
```
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir \
    --trusted-host pypi.org \
    --trusted-host files.pythonhosted.org \
    -r requirements.txt

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

 requirements.txt
```
css
코드 복사
fastapi
uvicorn[standard]
neo4j
pydantic
```

 실행 방법
1) 컨테이너 실행
```
docker-compose up -d --build
```
2) 상태 확인
```
docker ps
```
 API 테스트

✔ Swagger UI
브라우저에서:
```
http://localhost:8000/docs
```
✔ curl 

```
curl -X POST "http://localhost:8000/next" \
  -H "Content-Type: application/json" \
  -d '{"from_kw": "치킨", "to_kw": "닭고기"}'
```
 Neo4j 데이터 확인
Neo4j Browser:

arduino
코드 복사
http://localhost:7474
로그인

ID: neo4j

PW: 2Engussla

Cypher:


```
MATCH (a:Keyword)-[r:NEXT]->(b:Keyword)
RETURN a.name AS from_kw, r.count AS count, b.name AS to_kw
ORDER BY count DESC;
```
 Neo4j 컨테이너 재시작 문제 해결
```
Neo4j가 Restarting (1) 상태일 경우:

NEO4JLABS_PLUGINS 제거

기본 이미지로 실행


플러그인은 직접 다운로드하여 /neo4j/plugins에 넣어 사용

SSL 문제로 플러그인 자동 다운로드가 실패하면 Neo4j가 부팅되지 않습니다.
```
 목적 요약
```
Docker 기반 Neo4j + API 실행 템플릿

검색어 이동 흐름 저장용 API 제공

회사망 SSL 문제를 고려한 설치 방식

FastAPI와 Neo4j 연동 최소 예제 제공
```
