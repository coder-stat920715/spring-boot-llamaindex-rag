package com.example.rag.config;

import io.netty.channel.ChannelOption;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

/** WebClient tuned for long-lived SSE streams to the LlamaIndex sidecar. */
@Slf4j
@Configuration
public class WebClientConfig {

    @Bean(destroyMethod = "dispose")
    public ConnectionProvider sidecarConnectionProvider(RagProperties props) {
        return ConnectionProvider.builder("sidecar")
                .maxConnections(props.sidecar().maxConnections())
                .pendingAcquireTimeout(Duration.ofSeconds(10))
                .maxIdleTime(Duration.ofSeconds(30))
                .evictInBackground(Duration.ofSeconds(60))
                .build();
    }

    @Bean
    public WebClient sidecarWebClient(
            WebClient.Builder builder, ConnectionProvider sidecarConnectionProvider, RagProperties props) {
        RagProperties.Sidecar cfg = props.sidecar();
        HttpClient httpClient = HttpClient.create(sidecarConnectionProvider)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) cfg.connectTimeout().toMillis())
                .option(ChannelOption.SO_KEEPALIVE, true)
                .responseTimeout(cfg.responseTimeout())
                .compress(false); // compression would buffer SSE frames

        return builder.baseUrl(cfg.baseUrl())
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .codecs(c -> c.defaultCodecs().maxInMemorySize(cfg.maxInMemoryBytes()))
                .filter(ExchangeFilterFunction.ofRequestProcessor(req -> {
                    log.debug("sidecar request method={} url={}", req.method(), req.url());
                    return Mono.just(req);
                }))
                .build();
    }
}
