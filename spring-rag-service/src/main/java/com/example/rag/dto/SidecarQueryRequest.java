package com.example.rag.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Request body for the Python sidecar's streaming query endpoint. */
public record SidecarQueryRequest(String prompt, @JsonProperty("top_k") int topK) {}
