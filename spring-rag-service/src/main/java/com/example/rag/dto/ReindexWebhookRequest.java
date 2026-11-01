package com.example.rag.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

@Schema(description = "Webhook payload asking for a file in the watch directory to be re-indexed")
public record ReindexWebhookRequest(
        @NotBlank @Size(max = 255) @Schema(example = "handbook.pdf") String filename) {}
