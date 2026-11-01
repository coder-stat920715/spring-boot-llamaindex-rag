package com.example.rag.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "rag")
public record RagProperties(
        @Valid @NotNull Sidecar sidecar,
        @Valid @NotNull Ingestion ingestion,
        @Valid @NotNull Streaming streaming,
        @Valid @NotNull Watcher watcher,
        @Valid @NotNull VectorStore vectorStore) {

    public record Sidecar(
            @NotBlank String baseUrl,
            @NotNull Duration connectTimeout,
            @NotNull Duration responseTimeout,
            @Positive int maxConnections,
            @Positive int maxInMemoryBytes) {}

    public record Ingestion(@Positive long maxFileBytes, @NotEmpty List<String> allowedExtensions) {}

    public record Streaming(
            @NotNull Duration idleTimeout,
            @NotNull Duration heartbeat,
            @Positive int prefetch,
            @Min(1) int defaultTopK) {}

    public record Watcher(
            boolean enabled,
            @NotBlank String directory,
            @NotNull Duration debounce,
            String webhookSecret) {}

    public record VectorStore(
            @NotBlank String type, @NotBlank String host, @Positive int port, @NotBlank String collection) {}
}
