# Spring Boot + LlamaIndex RAG

    cp .env.example .env      # add OPENAI_API_KEY
    docker compose up --build

- Swagger UI:   http://localhost:8080/swagger-ui.html
- Ingest:       curl -F "file=@doc.pdf" http://localhost:8080/api/rag/ingest
- Stream:       curl -N "http://localhost:8080/api/rag/stream-query?prompt=Summarise%20the%20document"
- Auto-index:   drop files into ./data/watch
- Webhook:      curl -X POST -H "Content-Type: application/json" -d '{"filename":"doc.pdf"}' http://localhost:8080/api/rag/webhook/reindex
