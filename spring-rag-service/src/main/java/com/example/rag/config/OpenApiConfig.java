package com.example.rag.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI ragOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Spring RAG Gateway")
                .version("1.0.0")
                .description("Reactive gateway for document ingestion and streamed RAG answers backed by LlamaIndex."));
    }
}
