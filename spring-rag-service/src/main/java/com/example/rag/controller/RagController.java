package com.example.rag.controller;

import com.example.rag.config.RagProperties;
import com.example.rag.dto.ApiError;
import com.example.rag.dto.IngestResponse;
import com.example.rag.dto.ReindexWebhookRequest;
import com.example.rag.dto.StatusResponse;
import com.example.rag.service.DocumentWatcherService;
import com.example.rag.service.LlamaIndexBridgeService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
@RequestMapping("/api/rag")
@Tag(name = "RAG", description = "Document ingestion and streamed question answering")
public class RagController {

    private final LlamaIndexBridgeService bridge;
    private final DocumentWatcherService watcher;
    private final RagProperties props;

    public RagController(LlamaIndexBridgeService bridge, DocumentWatcherService watcher, RagProperties props) {
        this.bridge = bridge;
        this.watcher = watcher;
        this.props = props;
    }

    @Operation(summary = "Upload a document and index it immediately",
            description = "Streams the multipart file to the LlamaIndex sidecar. Re-uploading a file name replaces its previous chunks.")
    @ApiResponse(responseCode = "200", description = "Indexed")
    @ApiResponse(responseCode = "413", description = "File too large",
            content = @Content(schema = @Schema(implementation = ApiError.class)))
    @ApiResponse(responseCode = "415", description = "Unsupported file type",
            content = @Content(schema = @Schema(implementation = ApiError.class)))
    @ApiResponse(responseCode = "503", description = "Sidecar unavailable",
            content = @Content(schema = @Schema(implementation = ApiError.class)))
    @PostMapping(value = "/ingest", consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<IngestResponse> ingest(@RequestPart("file") FilePart file) {
        MediaType type = Optional.ofNullable(file.headers().getContentType()).orElse(MediaType.APPLICATION_OCTET_STREAM);
        log.info("ingest request file={} contentType={}", file.filename(), type);
        return bridge.ingest(file.filename(), type, file.content());
    }

    @Operation(summary = "Stream an answer token-by-token (Server-Sent Events)",
            description = "Events: `token` {token}, `sources` {sources[]}, `done`, `error` {code,message}. "
                    + "Comment lines (`: keep-alive`) are heartbeats.")
    @ApiResponse(responseCode = "200", description = "SSE stream",
            content = @Content(mediaType = MediaType.TEXT_EVENT_STREAM_VALUE))
    @GetMapping(value = "/stream-query", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> streamQuery(
            @Parameter(description = "Natural-language question", example = "What is our refund policy?")
            @RequestParam @NotBlank @Size(max = 4000) String prompt,
            @Parameter(description = "Number of chunks to retrieve")
            @RequestParam(required = false) @Min(1) @Max(20) Integer topK) {
        int k = topK != null ? topK : props.streaming().defaultTopK();
        log.info("stream-query promptChars={} topK={}", prompt.length(), k);
        return bridge.streamQuery(prompt, k);
    }

    @Operation(summary = "Webhook: re-index a file already present in the watch directory",
            description = "Send header `X-Webhook-Secret` when a secret is configured (rag.watcher.webhook-secret).")
    @ApiResponse(responseCode = "200", description = "Re-indexed")
    @ApiResponse(responseCode = "401", description = "Bad secret",
            content = @Content(schema = @Schema(implementation = ApiError.class)))
    @PostMapping(value = "/webhook/reindex", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<IngestResponse> reindexWebhook(
            @RequestHeader(value = "X-Webhook-Secret", required = false) String secret,
            @Valid @RequestBody ReindexWebhookRequest request) {
        if (!watcher.isAuthorized(secret)) {
            return Mono.error(new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid webhook secret"));
        }
        log.info("webhook reindex file={}", request.filename());
        return watcher.reindexByName(request.filename());
    }

    @Operation(summary = "Report sidecar and vector store status")
    @GetMapping(value = "/status", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<StatusResponse> status() {
        return bridge.status();
    }
}
