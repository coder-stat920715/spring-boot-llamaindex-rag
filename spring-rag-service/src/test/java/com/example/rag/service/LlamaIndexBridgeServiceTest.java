package com.example.rag.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.example.rag.config.RagProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class LlamaIndexBridgeServiceTest {

    private static RagProperties props() {
        return new RagProperties(
                new RagProperties.Sidecar("http://sidecar", Duration.ofSeconds(1), Duration.ofSeconds(5), 10, 1_048_576),
                new RagProperties.Ingestion(1024, List.of("pdf", "txt")),
                new RagProperties.Streaming(Duration.ofSeconds(5), Duration.ofSeconds(30), 16, 5),
                new RagProperties.Watcher(false, "./data/watch", Duration.ofMillis(500), ""),
                new RagProperties.VectorStore("qdrant", "localhost", 6333, "documents"));
    }

    @Test
    void streamQueryEmitsErrorEventWhenSidecarFails() {
        ExchangeFunction failing =
                request -> Mono.just(ClientResponse.create(HttpStatus.INTERNAL_SERVER_ERROR).body("boom").build());
        WebClient client = WebClient.builder().baseUrl("http://sidecar").exchangeFunction(failing).build();
        LlamaIndexBridgeService service = new LlamaIndexBridgeService(client, props(), new ObjectMapper());

        StepVerifier.create(service.streamQuery("hello", 3).filter(e -> e.event() != null))
                .assertNext(e -> assertEquals("error", e.event()))
                .verifyComplete();
    }

    @Test
    void ingestRejectsUnsupportedExtension() {
        WebClient client = WebClient.builder().baseUrl("http://sidecar").build();
        LlamaIndexBridgeService service = new LlamaIndexBridgeService(client, props(), new ObjectMapper());

        StepVerifier.create(service.ingest("malware.exe", null, reactor.core.publisher.Flux.empty()))
                .expectErrorMatches(ex -> ex.getMessage().contains("Unsupported"))
                .verify();
    }
}
