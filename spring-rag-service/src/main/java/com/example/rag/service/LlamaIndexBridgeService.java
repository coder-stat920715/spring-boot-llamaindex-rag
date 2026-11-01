package com.example.rag.service;

import com.example.rag.config.RagProperties;
import com.example.rag.dto.IngestResponse;
import com.example.rag.dto.SidecarQueryRequest;
import com.example.rag.dto.StatusResponse;
import com.example.rag.exception.SidecarException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/** Reactive bridge to the Python LlamaIndex sidecar. */
@Slf4j
@Service
public class LlamaIndexBridgeService {

    private static final ParameterizedTypeReference<ServerSentEvent<String>> SSE_TYPE =
            new ParameterizedTypeReference<>() {};

    private final WebClient sidecar;
    private final RagProperties props;
    private final ObjectMapper mapper;

    public LlamaIndexBridgeService(WebClient sidecarWebClient, RagProperties props, ObjectMapper mapper) {
        this.sidecar = sidecarWebClient;
        this.props = props;
        this.mapper = mapper;
    }

    public boolean isSupported(String filename) {
        return props.ingestion().allowedExtensions().contains(extension(filename));
    }

    /** Streams a document to the sidecar for (re-)indexing. The body is never fully buffered. */
    public Mono<IngestResponse> ingest(String filename, MediaType contentType, Flux<DataBuffer> content) {
        final String safeName;
        try {
            safeName = sanitize(filename);
        } catch (ResponseStatusException ex) {
            return Mono.error(ex);
        }
        if (!isSupported(safeName)) {
            return Mono.error(new ResponseStatusException(
                    HttpStatus.UNSUPPORTED_MEDIA_TYPE,
                    "Unsupported file type; allowed: " + props.ingestion().allowedExtensions()));
        }

        final long max = props.ingestion().maxFileBytes();
        Flux<DataBuffer> limited = Flux.defer(() -> {
            AtomicLong total = new AtomicLong();
            return content.<DataBuffer>handle((buf, sink) -> {
                if (total.addAndGet(buf.readableByteCount()) > max) {
                    DataBufferUtils.release(buf);
                    sink.error(new ResponseStatusException(
                            HttpStatus.PAYLOAD_TOO_LARGE, "File exceeds " + max + " bytes"));
                } else {
                    sink.next(buf);
                }
            });
        });

        MultipartBodyBuilder body = new MultipartBodyBuilder();
        body.asyncPart("file", limited, DataBuffer.class)
                .filename(safeName)
                .contentType(contentType == null ? MediaType.APPLICATION_OCTET_STREAM : contentType);

        log.info("ingest start file={}", safeName);
        return sidecar.post()
                .uri("/api/v1/index")
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(body.build()))
                .retrieve()
                .onStatus(HttpStatusCode::isError, this::toSidecarException)
                .bodyToMono(IngestResponse.class)
                .timeout(props.sidecar().responseTimeout())
                .doOnSuccess(r -> log.info("ingest done file={} chunks={}", safeName, r == null ? 0 : r.chunks()))
                .onErrorMap(this::mapError);
    }

    /**
     * Streams answer tokens as SSE. Sidecar failures never break the HTTP stream: they are
     * converted into a terminal {@code error} event so clients can render a graceful message.
     */
    public Flux<ServerSentEvent<String>> streamQuery(String prompt, int topK) {
        RagProperties.Streaming cfg = props.streaming();
        Flux<ServerSentEvent<String>> upstream = sidecar.post()
                .uri("/api/v1/query/stream")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(new SidecarQueryRequest(prompt, topK))
                .retrieve()
                .onStatus(HttpStatusCode::isError, this::toSidecarException)
                .bodyToFlux(SSE_TYPE)
                .retryWhen(Retry.backoff(2, Duration.ofMillis(300))
                        .filter(WebClientRequestException.class::isInstance)
                        .onRetryExhaustedThrow((spec, signal) -> signal.failure()))
                .timeout(cfg.idleTimeout())
                .limitRate(cfg.prefetch())
                .onErrorResume(ex -> {
                    Throwable mapped = mapError(ex);
                    log.warn("stream failed, emitting error event: {}", mapped.getMessage());
                    return Flux.just(errorEvent(mapped));
                })
                .doOnCancel(() -> log.info("client cancelled stream"));

        Flux<ServerSentEvent<String>> heartbeat = Flux.interval(cfg.heartbeat())
                .map(i -> ServerSentEvent.<String>builder().comment("keep-alive").build());

        return upstream.publish(shared ->
                Flux.merge(shared, heartbeat.takeUntilOther(shared.ignoreElements())));
    }

    public Mono<StatusResponse> status() {
        RagProperties.VectorStore vs = props.vectorStore();
        return sidecar.get()
                .uri("/health")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(n -> n.path("status").asText("UNKNOWN"))
                .timeout(Duration.ofSeconds(5))
                .onErrorReturn("UNAVAILABLE")
                .map(s -> new StatusResponse(s, vs.type(), vs.host() + ":" + vs.port(), vs.collection()));
    }

    private Mono<? extends Throwable> toSidecarException(ClientResponse response) {
        return response.bodyToMono(String.class).defaultIfEmpty("").map(body -> {
            HttpStatus status = response.statusCode().is4xxClientError()
                    ? HttpStatus.resolve(response.statusCode().value())
                    : HttpStatus.BAD_GATEWAY;
            return new SidecarException(
                    status == null ? HttpStatus.BAD_GATEWAY : status,
                    "Sidecar responded " + response.statusCode().value() + ": " + truncate(body));
        });
    }

    private Throwable mapError(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof ResponseStatusException || t instanceof SidecarException) {
                return t;
            }
            if (t instanceof WebClientRequestException) {
                return new SidecarException(HttpStatus.SERVICE_UNAVAILABLE, "LlamaIndex sidecar is unreachable", ex);
            }
            if (t instanceof TimeoutException) {
                return new SidecarException(HttpStatus.GATEWAY_TIMEOUT, "LlamaIndex sidecar timed out", ex);
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return new SidecarException(HttpStatus.BAD_GATEWAY, "Unexpected sidecar failure: " + ex.getMessage(), ex);
    }

    private ServerSentEvent<String> errorEvent(Throwable ex) {
        String json;
        try {
            json = mapper.writeValueAsString(Map.of("code", "UPSTREAM_ERROR", "message", String.valueOf(ex.getMessage())));
        } catch (JsonProcessingException e) {
            json = "{\"code\":\"UPSTREAM_ERROR\",\"message\":\"stream failed\"}";
        }
        return ServerSentEvent.<String>builder(json).event("error").build();
    }

    private static String sanitize(String filename) {
        if (filename == null || filename.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Missing filename");
        }
        try {
            String base = Paths.get(filename).getFileName().toString();
            String clean = base.replaceAll("[^A-Za-z0-9._ -]", "_").trim();
            if (clean.isEmpty() || clean.startsWith(".")) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid filename");
            }
            return clean;
        } catch (InvalidPathException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid filename");
        }
    }

    private static String extension(String filename) {
        if (filename == null) {
            return "";
        }
        int dot = filename.lastIndexOf('.');
        return dot < 0 ? "" : filename.substring(dot + 1).toLowerCase(Locale.ROOT);
    }

    private static String truncate(String s) {
        return s.length() > 300 ? s.substring(0, 300) + "..." : s;
    }
}
