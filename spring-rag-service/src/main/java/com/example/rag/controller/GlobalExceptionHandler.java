package com.example.rag.controller;

import com.example.rag.dto.ApiError;
import com.example.rag.exception.SidecarException;
import java.time.Instant;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.MessageSourceResolvable;
import org.springframework.core.io.buffer.DataBufferLimitException;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(SidecarException.class)
    public ResponseEntity<ApiError> handleSidecar(SidecarException ex, ServerWebExchange exchange) {
        log.warn("sidecar error status={} msg={}", ex.getStatus().value(), ex.getMessage());
        return build(ex.getStatus(), ex.getMessage(), exchange);
    }

    @ExceptionHandler(HandlerMethodValidationException.class)
    public ResponseEntity<ApiError> handleValidation(HandlerMethodValidationException ex, ServerWebExchange exchange) {
        String message = ex.getAllValidationResults().stream()
                .flatMap(r -> r.getResolvableErrors().stream())
                .map(MessageSourceResolvable::getDefaultMessage)
                .filter(Objects::nonNull)
                .collect(Collectors.joining("; "));
        return build(HttpStatus.BAD_REQUEST, message.isBlank() ? "Validation failed" : message, exchange);
    }

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ApiError> handleStatus(ResponseStatusException ex, ServerWebExchange exchange) {
        return build(ex.getStatusCode(), ex.getReason() == null ? ex.getMessage() : ex.getReason(), exchange);
    }

    @ExceptionHandler(DataBufferLimitException.class)
    public ResponseEntity<ApiError> handleLimit(DataBufferLimitException ex, ServerWebExchange exchange) {
        return build(HttpStatus.PAYLOAD_TOO_LARGE, "Payload exceeds configured limit", exchange);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleUnexpected(Exception ex, ServerWebExchange exchange) {
        log.error("unhandled error path={}", exchange.getRequest().getPath(), ex);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error", exchange);
    }

    private ResponseEntity<ApiError> build(HttpStatusCode status, String message, ServerWebExchange exchange) {
        HttpStatus resolved = HttpStatus.resolve(status.value());
        ApiError body = new ApiError(
                Instant.now(),
                status.value(),
                resolved == null ? "Error" : resolved.getReasonPhrase(),
                message,
                exchange.getRequest().getPath().value());
        return ResponseEntity.status(status).body(body);
    }
}
