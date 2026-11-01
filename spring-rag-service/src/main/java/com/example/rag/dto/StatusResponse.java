package com.example.rag.dto;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Gateway view of the RAG backend")
public record StatusResponse(
        @Schema(example = "UP") String sidecar,
        @Schema(example = "qdrant") String vectorStore,
        @Schema(example = "qdrant:6333") String endpoint,
        @Schema(example = "documents") String collection) {}
