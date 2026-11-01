package com.example.rag.exception;

import org.springframework.http.HttpStatus;

/** Failure talking to the LlamaIndex sidecar; carries the HTTP status to expose to clients. */
public class SidecarException extends RuntimeException {

    private final HttpStatus status;

    public SidecarException(HttpStatus status, String message) {
        super(message);
        this.status = status;
    }

    public SidecarException(HttpStatus status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    public HttpStatus getStatus() {
        return status;
    }
}
