package com.example.rag.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Result of indexing a document")
public record IngestResponse(
        @Schema(example = "handbook.pdf") String filename,
        @Schema(example = "42") int chunks,
        @Schema(example = "INDEXED") String status,
        @JsonProperty("elapsed_ms") @Schema(example = "1830") long elapsedMs) {}
