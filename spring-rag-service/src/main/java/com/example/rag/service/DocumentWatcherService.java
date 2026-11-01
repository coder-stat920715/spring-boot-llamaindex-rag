package com.example.rag.service;

import com.example.rag.config.RagProperties;
import com.example.rag.dto.IngestResponse;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.security.MessageDigest;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/** Watches a directory and triggers real-time re-indexing; also backs the reindex webhook. */
@Slf4j
@Service
public class DocumentWatcherService {

    private final LlamaIndexBridgeService bridge;
    private final RagProperties.Watcher cfg;
    private final Path watchDir;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("rag-debounce").daemon().factory());
    private final Map<Path, ScheduledFuture<?>> pending = new ConcurrentHashMap<>();
    private volatile WatchService watchService;

    public DocumentWatcherService(LlamaIndexBridgeService bridge, RagProperties props) {
        this.bridge = bridge;
        this.cfg = props.watcher();
        this.watchDir = Path.of(cfg.directory()).toAbsolutePath().normalize();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (!cfg.enabled()) {
            log.info("file watcher disabled");
            return;
        }
        try {
            Files.createDirectories(watchDir);
            watchService = FileSystems.getDefault().newWatchService();
            watchDir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
            Thread.ofPlatform().name("rag-file-watcher").daemon().start(this::loop);
            log.info("watching directory={}", watchDir);
        } catch (IOException ex) {
            log.error("could not start file watcher directory={}", watchDir, ex);
        }
    }

    @PreDestroy
    public void stop() {
        scheduler.shutdownNow();
        try {
            if (watchService != null) {
                watchService.close();
            }
        } catch (IOException ex) {
            log.warn("error closing watch service", ex);
        }
    }

    /** Constant-time check of the webhook shared secret; an empty configured secret disables the check. */
    public boolean isAuthorized(String provided) {
        String expected = cfg.webhookSecret();
        if (expected == null || expected.isBlank()) {
            return true;
        }
        return provided != null && MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8), provided.getBytes(StandardCharsets.UTF_8));
    }

    /** Re-indexes a file that lives inside the watch directory. */
    public Mono<IngestResponse> reindexByName(String filename) {
        final Path resolved;
        try {
            resolved = watchDir.resolve(filename).normalize();
        } catch (InvalidPathException ex) {
            return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid filename"));
        }
        if (!resolved.startsWith(watchDir)) {
            return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "Path escapes watch directory"));
        }
        return ingestPath(resolved);
    }

    Mono<IngestResponse> ingestPath(Path file) {
        return Mono.fromCallable(() -> {
                    if (!Files.isRegularFile(file) || Files.isHidden(file)) {
                        throw new ResponseStatusException(HttpStatus.NOT_FOUND, "File not found: " + file.getFileName());
                    }
                    String probed = Files.probeContentType(file);
                    return probed == null ? MediaType.APPLICATION_OCTET_STREAM : MediaType.parseMediaType(probed);
                })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(mediaType -> bridge.ingest(
                        file.getFileName().toString(),
                        mediaType,
                        DataBufferUtils.read(file, DefaultDataBufferFactory.sharedInstance, 16 * 1024)));
    }

    private void loop() {
        while (!Thread.currentThread().isInterrupted()) {
            WatchKey key;
            try {
                key = watchService.take();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            } catch (ClosedWatchServiceException ex) {
                return;
            }
            for (WatchEvent<?> event : key.pollEvents()) {
                if (event.kind() == StandardWatchEventKinds.OVERFLOW) {
                    log.warn("watch events overflowed; some files may need a manual reindex");
                    continue;
                }
                Path file = watchDir.resolve((Path) event.context());
                if (bridge.isSupported(file.getFileName().toString())) {
                    schedule(file);
                }
            }
            if (!key.reset()) {
                log.warn("watch key invalidated; stopping watcher");
                return;
            }
        }
    }

    /** Debounce: editors and uploads fire several events per file. */
    private void schedule(Path file) {
        pending.compute(file, (p, previous) -> {
            if (previous != null) {
                previous.cancel(false);
            }
            return scheduler.schedule(() -> {
                pending.remove(p);
                ingestPath(p).subscribe(
                        r -> log.info("auto-indexed file={} chunks={}", r.filename(), r.chunks()),
                        e -> log.error("auto-index failed file={} cause={}", p.getFileName(), e.getMessage()));
            }, cfg.debounce().toMillis(), TimeUnit.MILLISECONDS);
        });
    }
}
