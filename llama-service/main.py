"""LlamaIndex RAG sidecar: document indexing and token-streaming queries over Qdrant."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse
from llama_index.core import Document, Settings, SimpleDirectoryReader, VectorStoreIndex
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.prompts import PromptTemplate
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.llms.openai import OpenAI
from llama_index.vector_stores.qdrant import QdrantVectorStore
from llama_parse import LlamaParse
from pydantic import BaseModel, Field
from qdrant_client import AsyncQdrantClient, QdrantClient
from qdrant_client.http import models as qm

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
)
log = logging.getLogger("llama-service")


class Config:
    qdrant_url = os.getenv("QDRANT_URL", "http://localhost:6333")
    qdrant_api_key = os.getenv("QDRANT_API_KEY") or None
    collection = os.getenv("QDRANT_COLLECTION", "documents")
    openai_api_key = os.getenv("OPENAI_API_KEY", "")
    llm_model = os.getenv("LLM_MODEL", "gpt-4o-mini")
    embed_model = os.getenv("EMBED_MODEL", "text-embedding-3-small")
    llama_cloud_api_key = os.getenv("LLAMA_CLOUD_API_KEY", "")
    chunk_size = int(os.getenv("CHUNK_SIZE", "512"))
    chunk_overlap = int(os.getenv("CHUNK_OVERLAP", "64"))
    max_upload_bytes = int(os.getenv("MAX_UPLOAD_MB", "50")) * 1024 * 1024


cfg = Config()

LOCAL_EXTENSIONS = {"txt", "md", "pdf"}
PARSER_EXTENSIONS = {"docx", "pptx", "xlsx", "html", "csv"}

QA_PROMPT = PromptTemplate(
    "You are a precise assistant. Answer using only the context below. "
    "If the context is insufficient, say you do not know.\n"
    "---------------------\n{context_str}\n---------------------\n"
    "Question: {query_str}\nAnswer: "
)


class QueryRequest(BaseModel):
    prompt: str = Field(min_length=1, max_length=4000)
    top_k: int = Field(default=5, ge=1, le=20)


class IndexResponse(BaseModel):
    filename: str
    chunks: int
    status: str
    elapsed_ms: int


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    if not cfg.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")
    Settings.llm = OpenAI(model=cfg.llm_model, temperature=0.1, api_key=cfg.openai_api_key)
    Settings.embed_model = OpenAIEmbedding(model=cfg.embed_model, api_key=cfg.openai_api_key)

    client = QdrantClient(url=cfg.qdrant_url, api_key=cfg.qdrant_api_key)
    aclient = AsyncQdrantClient(url=cfg.qdrant_url, api_key=cfg.qdrant_api_key)
    vector_store = QdrantVectorStore(collection_name=cfg.collection, client=client, aclient=aclient)

    app.state.client = client
    app.state.aclient = aclient
    app.state.index = VectorStoreIndex.from_vector_store(vector_store=vector_store)
    app.state.splitter = SentenceSplitter(chunk_size=cfg.chunk_size, chunk_overlap=cfg.chunk_overlap)
    app.state.parser = (
        LlamaParse(api_key=cfg.llama_cloud_api_key, result_type="markdown")
        if cfg.llama_cloud_api_key
        else None
    )
    app.state.locks = defaultdict(asyncio.Lock)
    log.info("started collection=%s llamaparse=%s", cfg.collection, app.state.parser is not None)
    try:
        yield
    finally:
        await aclient.close()
        client.close()


app = FastAPI(title="LlamaIndex RAG Sidecar", version="1.0.0", lifespan=lifespan)


def safe_name(raw: str | None) -> str:
    name = Path(raw or "").name
    name = re.sub(r"[^A-Za-z0-9._ -]", "_", name).strip()
    if not name or name.startswith("."):
        raise HTTPException(status_code=400, detail="Invalid filename")
    return name


def allowed_extensions(state: Any) -> set[str]:
    return LOCAL_EXTENSIONS | (PARSER_EXTENSIONS if state.parser else set())


async def load_documents(path: Path, state: Any) -> list[Document]:
    ext = path.suffix.lstrip(".").lower()
    if state.parser is not None and ext in (PARSER_EXTENSIONS | {"pdf"}):
        return await state.parser.aload_data(str(path))
    return await asyncio.to_thread(SimpleDirectoryReader(input_files=[str(path)]).load_data)


def delete_existing(client: QdrantClient, name: str) -> None:
    if not client.collection_exists(cfg.collection):
        return
    client.delete(
        collection_name=cfg.collection,
        points_selector=qm.FilterSelector(
            filter=qm.Filter(
                must=[qm.FieldCondition(key="source_file", match=qm.MatchValue(value=name))]
            )
        ),
        wait=True,
    )


@app.get("/health")
async def health(request: Request) -> JSONResponse:
    try:
        await request.app.state.aclient.get_collections()
        return JSONResponse({"status": "UP"})
    except Exception as exc:  # noqa: BLE001
        log.error("health check failed: %s", exc)
        return JSONResponse({"status": "DOWN"}, status_code=503)


@app.post("/api/v1/index", response_model=IndexResponse)
async def index_document(request: Request, file: UploadFile = File(...)) -> IndexResponse:
    started = time.perf_counter()
    state = request.app.state
    name = safe_name(file.filename)
    ext = Path(name).suffix.lstrip(".").lower()
    if ext not in allowed_extensions(state):
        raise HTTPException(status_code=415, detail=f"Unsupported file type '.{ext}'")

    tmpdir = tempfile.mkdtemp(prefix="ingest-")
    try:
        path = Path(tmpdir) / name
        size = 0
        with path.open("wb") as out:
            while chunk := await file.read(1024 * 1024):
                size += len(chunk)
                if size > cfg.max_upload_bytes:
                    raise HTTPException(status_code=413, detail="File too large")
                out.write(chunk)
        if size == 0:
            raise HTTPException(status_code=400, detail="Empty file")

        try:
            docs = await load_documents(path, state)
        except Exception as exc:  # noqa: BLE001
            log.exception("parse failed file=%s", name)
            raise HTTPException(status_code=422, detail=f"Could not parse document: {exc}") from exc

        for doc in docs:
            doc.metadata["source_file"] = name
        nodes = [n for n in state.splitter.get_nodes_from_documents(docs) if n.get_content().strip()]
        if not nodes:
            raise HTTPException(status_code=422, detail="Document contains no extractable text")

        async with state.locks[name]:  # serialise re-index of the same file
            await asyncio.to_thread(delete_existing, state.client, name)
            await asyncio.to_thread(state.index.insert_nodes, nodes)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    elapsed = int((time.perf_counter() - started) * 1000)
    log.info("indexed file=%s chunks=%d elapsed_ms=%d", name, len(nodes), elapsed)
    return IndexResponse(filename=name, chunks=len(nodes), status="INDEXED", elapsed_ms=elapsed)


def sse(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


async def answer_stream(state: Any, body: QueryRequest) -> AsyncIterator[str]:
    try:
        if not await state.aclient.collection_exists(cfg.collection):
            yield sse("error", {"code": "NO_DOCUMENTS", "message": "No documents indexed yet"})
            return
        engine = state.index.as_query_engine(
            streaming=True, similarity_top_k=body.top_k, text_qa_template=QA_PROMPT
        )
        response = await engine.aquery(body.prompt)
        async for token in response.async_response_gen():
            yield sse("token", {"token": token})
        sources = [
            {"file": n.node.metadata.get("source_file"), "score": n.score}
            for n in (getattr(response, "source_nodes", None) or [])
        ]
        yield sse("sources", {"sources": sources})
        yield sse("done", {})
    except asyncio.CancelledError:
        log.info("client disconnected, aborting stream")
        raise
    except Exception as exc:  # noqa: BLE001
        log.exception("query failed")
        yield sse("error", {"code": "QUERY_FAILED", "message": str(exc)})


@app.post("/api/v1/query/stream")
async def query_stream(request: Request, body: QueryRequest) -> StreamingResponse:
    return StreamingResponse(
        answer_stream(request.app.state, body),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )
