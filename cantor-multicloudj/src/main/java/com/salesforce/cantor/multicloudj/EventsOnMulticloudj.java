/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.multicloudj;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.salesforce.cantor.Events;
import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.EventsPreconditions.*;

public final class EventsOnMulticloudj extends AbstractBaseMulticloudjNamespaceable
        implements Events, AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(EventsOnMulticloudj.class);

    static final String OBJECT_KEY_PREFIX = "cantor-events";
    private static final long DEFAULT_FLUSH_SECONDS = 60;
    private static final String DEFAULT_BUFFER_DIRECTORY = "cantor-events-multicloudj-buffer";
    // DIRECTORY_FORMATTER_MIN_PATTERN is the canonical definition in MulticloudjUtils;
    // re-exposed here as a package-private constant to avoid duplicate string literals.
    static final String DIRECTORY_FORMATTER_MIN_PATTERN = MulticloudjUtils.DIRECTORY_FORMATTER_MIN_PATTERN;
    private static final String CYCLE_NAME_FORMATTER_PATTERN = "yyyy-MM-dd_HH-mm-ss";
    private static final String DIMENSION_KEY_PAYLOAD_OFFSET = ".cantor-payload-offset";
    private static final String DIMENSION_KEY_PAYLOAD_LENGTH = ".cantor-payload-length";

    private final String instanceId;
    private final Path bufferRoot;
    private final ScheduledExecutorService flushExecutor;
    private final ExecutorService uploadPool;
    private final Map<String, ReentrantLock> namespaceLocks = new ConcurrentHashMap<>();
    private final LoadingCache<String, AtomicLong> payloadOffsets;
    private final AtomicReference<String> currentFlushCycle;
    private final Gson parser = new GsonBuilder().create();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public EventsOnMulticloudj(final BucketClient bucketClient) throws IOException {
        this(bucketClient, DEFAULT_BUFFER_DIRECTORY, DEFAULT_FLUSH_SECONDS);
    }

    public EventsOnMulticloudj(final BucketClient bucketClient, final String bufferDirectory) throws IOException {
        this(bucketClient, bufferDirectory, DEFAULT_FLUSH_SECONDS);
    }

    public EventsOnMulticloudj(final BucketClient bucketClient,
                               final String bufferDirectory,
                               final long flushIntervalSeconds) throws IOException {
        super(bucketClient, "events");
        checkArgument(flushIntervalSeconds > 0, "invalid flush interval");
        checkString(bufferDirectory, "invalid buffer directory");

        // F3 (CWE-22): validate + canonicalize the buffer directory path before use.
        // Reject any input containing ".." segments to prevent path traversal, and refuse
        // paths that resolve to an existing non-directory (e.g. a file or symlink target).
        final Path validatedBufferRoot = validateAndCanonicalizeBufferDirectory(bufferDirectory);

        this.instanceId = UUID.randomUUID().toString().replace("-", "");
        this.bufferRoot = validatedBufferRoot.resolve(this.instanceId);

        // F2 (CWE-276 / CWE-200): create the buffer directory with restrictive perms so
        // buffered events (which may contain PII) are not world-readable on POSIX systems.
        createBufferRootWithRestrictivePermissions(this.bufferRoot);

        this.payloadOffsets = CacheBuilder.newBuilder()
                .build(new CacheLoader<String, AtomicLong>() {
                    @Override
                    public AtomicLong load(final String ignoredPath) {
                        return new AtomicLong(0);
                    }
                });

        SimpleDateFormat cycleFormatter = new SimpleDateFormat(CYCLE_NAME_FORMATTER_PATTERN);
        cycleFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        this.currentFlushCycle = new AtomicReference<>(
                cycleFormatter.format(new Date(System.currentTimeMillis())) + "." + this.instanceId
        );

        this.uploadPool = Executors.newFixedThreadPool(32, new ThreadFactory() {
            private int count = 0;
            @Override
            public synchronized Thread newThread(Runnable r) {
                Thread t = new Thread(r, "cantor-multicloudj-upload-" + instanceId.substring(0, 8) + "-" + count++);
                t.setDaemon(true);
                return t;
            }
        });

        this.flushExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cantor-multicloudj-flusher-" + instanceId.substring(0, 8));
            t.setDaemon(true);
            return t;
        });
        this.flushExecutor.scheduleWithFixedDelay(this::flush, flushIntervalSeconds, flushIntervalSeconds, TimeUnit.SECONDS);
    }

    @Override
    public void store(final String namespace, final Collection<Event> batch) throws IOException {
        checkStore(namespace, batch);
        checkNamespaceExists(namespace);
        try {
            doStore(namespace, batch);
        } catch (SubstrateSdkException e) {
            throw MulticloudjUtils.wrapAsIOException("store", namespace, e);
        }
    }

    @Override
    public List<Event> get(final String namespace,
                           final long startTimestampMillis,
                           final long endTimestampMillis,
                           final Map<String, String> metadataQuery,
                           final Map<String, String> dimensionsQuery,
                           final boolean includePayloads,
                           final boolean ascending,
                           final int limit) throws IOException {
        checkGet(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        checkNamespaceExists(namespace);
        flushBeforeRead();
        try {
            return doGet(namespace, startTimestampMillis, endTimestampMillis,
                    metadataQuery != null ? metadataQuery : Collections.emptyMap(),
                    dimensionsQuery != null ? dimensionsQuery : Collections.emptyMap(),
                    includePayloads, ascending, limit);
        } catch (SubstrateSdkException | InterruptedException e) {
            throw MulticloudjUtils.wrapAsIOException("get", namespace, e);
        }
    }

    @Override
    public Set<String> metadata(final String namespace,
                                final String metadataKey,
                                final long startTimestampMillis,
                                final long endTimestampMillis,
                                final Map<String, String> metadataQuery,
                                final Map<String, String> dimensionsQuery) throws IOException {
        checkMetadata(namespace, metadataKey, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        checkNamespaceExists(namespace);
        flushBeforeRead();
        try {
            return doMetadata(namespace, metadataKey, startTimestampMillis, endTimestampMillis,
                    metadataQuery != null ? metadataQuery : Collections.emptyMap(),
                    dimensionsQuery != null ? dimensionsQuery : Collections.emptyMap());
        } catch (SubstrateSdkException | InterruptedException e) {
            throw MulticloudjUtils.wrapAsIOException("metadata", namespace, e);
        }
    }

    @Override
    public List<Event> dimension(final String namespace,
                                 final String dimensionKey,
                                 final long startTimestampMillis,
                                 final long endTimestampMillis,
                                 final Map<String, String> metadataQuery,
                                 final Map<String, String> dimensionsQuery) throws IOException {
        checkDimension(namespace, dimensionKey, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        checkNamespaceExists(namespace);
        flushBeforeRead();
        try {
            return doDimension(namespace, dimensionKey, startTimestampMillis, endTimestampMillis,
                    metadataQuery != null ? metadataQuery : Collections.emptyMap(),
                    dimensionsQuery != null ? dimensionsQuery : Collections.emptyMap());
        } catch (SubstrateSdkException | InterruptedException e) {
            throw MulticloudjUtils.wrapAsIOException("dimension", namespace, e);
        }
    }

    @Override
    public void expire(final String namespace, final long endTimestampMillis) throws IOException {
        checkExpire(namespace, endTimestampMillis);
        checkNamespaceExists(namespace);
        // F4 (correctness / R-D): flush buffered writes before computing the prefix
        // list to delete. Otherwise events that were still in the in-memory buffer
        // and within the expiry window survive the call and reappear on later reads.
        // Matches the pattern used by get(), metadata(), and dimension().
        flushBeforeRead();
        try {
            doExpire(namespace, endTimestampMillis);
        } catch (SubstrateSdkException e) {
            throw MulticloudjUtils.wrapAsIOException("expire", namespace, e);
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        flushExecutor.shutdown();
        try {
            if (!flushExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                flushExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            flushExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        uploadPool.shutdown();
        try {
            if (!uploadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                uploadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            uploadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    void forceFlushNow() {
        flushBeforeRead();
    }

    private void flushBeforeRead() {
        doFlush(false);
    }

    @Override
    protected String getObjectKeyPrefix(final String namespace) {
        return String.format("%s/%s", OBJECT_KEY_PREFIX, trim(namespace));
    }

    private void doStore(final String namespace, final Collection<Event> batch) throws IOException {
        SimpleDateFormat minFormatter = new SimpleDateFormat(DIRECTORY_FORMATTER_MIN_PATTERN);
        minFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        ReentrantLock lock = namespaceLocks.computeIfAbsent(namespace, k -> new ReentrantLock());
        lock.lock();
        try {
            String cycleName = currentFlushCycle.get();
            for (Event event : batch) {
                appendEvent(namespace, event, cycleName, minFormatter);
            }
        } finally {
            lock.unlock();
        }
    }

    private void appendEvent(final String namespace, final Event event,
                             final String cycleName, final SimpleDateFormat minFormatter) throws IOException {
        Map<String, String> metadata = event.getMetadata() != null ? new HashMap<>(event.getMetadata()) : new HashMap<>();
        Map<String, Double> dimensions = event.getDimensions() != null ? new HashMap<>(event.getDimensions()) : new HashMap<>();
        byte[] payload = event.getPayload();

        String timeDir = minFormatter.format(new Date(event.getTimestampMillis()));
        Path cycleDir = bufferRoot.resolve(cycleName);
        Path namespaceDir = cycleDir.resolve(OBJECT_KEY_PREFIX).resolve(trim(namespace)).resolve(timeDir);
        Files.createDirectories(namespaceDir);

        String fileBase = timeDir.substring(timeDir.lastIndexOf('/') + 1) + "." + cycleName;
        Path eventsFile = namespaceDir.resolve(fileBase + ".json");
        Path payloadFile = namespaceDir.resolve(fileBase + ".b64");

        if (payload != null && payload.length > 0) {
            String payloadBase64 = Base64.getEncoder().encodeToString(payload);
            String payloadLine = payloadBase64 + "\n";
            long offset = payloadOffsets.getUnchecked(payloadFile.toString()).getAndAdd(payloadLine.length());
            dimensions.put(DIMENSION_KEY_PAYLOAD_OFFSET, (double) offset);
            dimensions.put(DIMENSION_KEY_PAYLOAD_LENGTH, (double) payloadBase64.length());
            Files.write(payloadFile, payloadLine.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }

        Event toWrite = new Event(event.getTimestampMillis(), metadata, dimensions);
        String jsonLine = parser.toJson(toWrite) + "\n";
        Files.write(eventsFile, jsonLine.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private void flush() {
        doFlush(true);
    }

    private void doFlush(boolean waitForInflight) {
        long startMillis = System.currentTimeMillis();
        try {
            SimpleDateFormat cycleFormatter = new SimpleDateFormat(CYCLE_NAME_FORMATTER_PATTERN);
            cycleFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
            String newCycle = cycleFormatter.format(new Date(System.currentTimeMillis())) + "." + instanceId;
            String oldCycle = currentFlushCycle.getAndSet(newCycle);

            if (waitForInflight) {
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            Path oldCycleDir = bufferRoot.resolve(oldCycle);
            if (!Files.exists(oldCycleDir) || !Files.isDirectory(oldCycleDir)) {
                return;
            }

            List<CompletableFuture<Void>> futures = new ArrayList<>();
            Files.walkFileTree(oldCycleDir, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    Path relativePath = oldCycleDir.resolve(OBJECT_KEY_PREFIX).getParent().relativize(file);
                    String blobKey = relativePath.toString().replace(File.separatorChar, '/');

                    futures.add(CompletableFuture.runAsync(() -> {
                        try {
                            MulticloudjUtils.uploadFile(bucketClient, blobKey, file.toFile());
                        } catch (IOException e) {
                            throw new CompletionException(e);
                        }
                    }, uploadPool));
                    return FileVisitResult.CONTINUE;
                }
            });

            boolean allSucceeded = true;
            for (CompletableFuture<Void> f : futures) {
                try {
                    f.get(60, TimeUnit.SECONDS);
                } catch (Exception e) {
                    allSucceeded = false;
                    logger.warn("Failed to upload buffer file during flush cycle '{}': {}", oldCycle, e.getMessage());
                }
            }

            if (allSucceeded) {
                deleteDirectoryRecursively(oldCycleDir);
            }
        } catch (Exception e) {
            logger.warn("Exception during flush", e);
        } finally {
            long elapsed = System.currentTimeMillis() - startMillis;
            logger.debug("Flush cycle elapsed: {}ms", elapsed);
        }
    }

    private List<Event> doGet(final String namespace,
                              final long startTimestampMillis,
                              final long endTimestampMillis,
                              final Map<String, String> metadataQuery,
                              final Map<String, String> dimensionsQuery,
                              final boolean includePayloads,
                              final boolean ascending,
                              final int limit) throws IOException, InterruptedException {

        Set<String> prefixes = MulticloudjUtils.getMatchingKeys(bucketClient, getObjectKeyPrefix(namespace), startTimestampMillis, endTimestampMillis);
        List<String> jsonKeys = new ArrayList<>();
        for (String prefix : prefixes) {
            for (String key : MulticloudjUtils.listKeys(bucketClient, prefix)) {
                if (key.endsWith(".json")) {
                    jsonKeys.add(key);
                }
            }
        }

        ConcurrentLinkedQueue<Event> results = new ConcurrentLinkedQueue<>();
        AtomicBoolean hasFailed = new AtomicBoolean(false);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (String objectKey : jsonKeys) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    List<Event> events = doGetOnObject(objectKey, startTimestampMillis, endTimestampMillis,
                            metadataQuery, dimensionsQuery, includePayloads);
                    results.addAll(events);
                } catch (IOException e) {
                    hasFailed.set(true);
                    logger.warn("Exception on get call for key '{}': {}", objectKey, e.getMessage());
                }
            }, uploadPool));
        }

        for (CompletableFuture<Void> f : futures) {
            try {
                f.get(60, TimeUnit.SECONDS);
            } catch (ExecutionException | TimeoutException e) {
                hasFailed.set(true);
            }
        }

        if (hasFailed.get()) {
            throw new IOException("exception on get call to blob storage");
        }

        List<Event> sorted = new ArrayList<>(results);
        sorted.sort((e1, e2) -> {
            if (e1.getTimestampMillis() < e2.getTimestampMillis()) return ascending ? -1 : 1;
            if (e1.getTimestampMillis() > e2.getTimestampMillis()) return ascending ? 1 : -1;
            return 0;
        });

        if (limit > 0) {
            return sorted.subList(0, Math.min(limit, sorted.size()));
        }
        return sorted;
    }

    private List<Event> doGetOnObject(final String objectKey,
                                      final long startTimestampMillis,
                                      final long endTimestampMillis,
                                      final Map<String, String> metadataQuery,
                                      final Map<String, String> dimensionsQuery,
                                      final boolean includePayloads) throws IOException {
        List<Event> results = new ArrayList<>();
        byte[] content = MulticloudjUtils.download(bucketClient, objectKey);
        String[] lines = new String(content, StandardCharsets.UTF_8).split("\n");

        for (String line : lines) {
            if (line.trim().isEmpty()) continue;
            Event event = parser.fromJson(line, Event.class);
            if (event.getTimestampMillis() < startTimestampMillis || event.getTimestampMillis() > endTimestampMillis) {
                continue;
            }
            if (!MulticloudjUtils.matchesMetadata(event.getMetadata(), metadataQuery)) {
                continue;
            }
            if (!MulticloudjUtils.matchesDimensions(event.getDimensions(), dimensionsQuery)) {
                continue;
            }

            if (includePayloads
                    && event.getDimensions().containsKey(DIMENSION_KEY_PAYLOAD_OFFSET)
                    && event.getDimensions().containsKey(DIMENSION_KEY_PAYLOAD_LENGTH)) {
                long offset = event.getDimensions().get(DIMENSION_KEY_PAYLOAD_OFFSET).longValue();
                long length = event.getDimensions().get(DIMENSION_KEY_PAYLOAD_LENGTH).longValue();
                String payloadKey = objectKey.substring(0, objectKey.lastIndexOf(".json")) + ".b64";
                byte[] payloadBase64Bytes = MulticloudjUtils.downloadRange(bucketClient, payloadKey, offset, offset + length - 1);
                byte[] payload = Base64.getDecoder().decode(new String(payloadBase64Bytes, StandardCharsets.UTF_8).trim());
                results.add(new Event(event.getTimestampMillis(), event.getMetadata(), event.getDimensions(), payload));
            } else {
                results.add(event);
            }
        }
        return results;
    }

    private Set<String> doMetadata(final String namespace,
                                   final String metadataKey,
                                   final long startTimestampMillis,
                                   final long endTimestampMillis,
                                   final Map<String, String> metadataQuery,
                                   final Map<String, String> dimensionsQuery) throws IOException, InterruptedException {

        Set<String> prefixes = MulticloudjUtils.getMatchingKeys(bucketClient, getObjectKeyPrefix(namespace), startTimestampMillis, endTimestampMillis);
        Set<String> results = ConcurrentHashMap.newKeySet();
        AtomicBoolean hasFailed = new AtomicBoolean(false);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (String prefix : prefixes) {
            for (String key : MulticloudjUtils.listKeys(bucketClient, prefix)) {
                if (!key.endsWith(".json")) continue;
                futures.add(CompletableFuture.runAsync(() -> {
                    try {
                        byte[] content = MulticloudjUtils.download(bucketClient, key);
                        String[] lines = new String(content, StandardCharsets.UTF_8).split("\n");
                        for (String line : lines) {
                            if (line.trim().isEmpty()) continue;
                            Event event = parser.fromJson(line, Event.class);
                            if (event.getTimestampMillis() < startTimestampMillis || event.getTimestampMillis() > endTimestampMillis) continue;
                            if (!MulticloudjUtils.matchesMetadata(event.getMetadata(), metadataQuery)) continue;
                            if (!MulticloudjUtils.matchesDimensions(event.getDimensions(), dimensionsQuery)) continue;
                            String val = event.getMetadata().get(metadataKey);
                            if (val != null) results.add(val);
                        }
                    } catch (IOException e) {
                        hasFailed.set(true);
                        logger.warn("Exception on metadata call for key '{}': {}", key, e.getMessage());
                    }
                }, uploadPool));
            }
        }

        for (CompletableFuture<Void> f : futures) {
            try {
                f.get(60, TimeUnit.SECONDS);
            } catch (ExecutionException | TimeoutException e) {
                hasFailed.set(true);
            }
        }

        if (hasFailed.get()) {
            throw new IOException("exception on metadata call to blob storage");
        }
        return results;
    }

    private List<Event> doDimension(final String namespace,
                                    final String dimensionKey,
                                    final long startTimestampMillis,
                                    final long endTimestampMillis,
                                    final Map<String, String> metadataQuery,
                                    final Map<String, String> dimensionsQuery) throws IOException, InterruptedException {

        Set<String> prefixes = MulticloudjUtils.getMatchingKeys(bucketClient, getObjectKeyPrefix(namespace), startTimestampMillis, endTimestampMillis);
        ConcurrentLinkedQueue<Event> results = new ConcurrentLinkedQueue<>();
        AtomicBoolean hasFailed = new AtomicBoolean(false);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (String prefix : prefixes) {
            for (String key : MulticloudjUtils.listKeys(bucketClient, prefix)) {
                if (!key.endsWith(".json")) continue;
                futures.add(CompletableFuture.runAsync(() -> {
                    try {
                        byte[] content = MulticloudjUtils.download(bucketClient, key);
                        String[] lines = new String(content, StandardCharsets.UTF_8).split("\n");
                        for (String line : lines) {
                            if (line.trim().isEmpty()) continue;
                            Event event = parser.fromJson(line, Event.class);
                            if (event.getTimestampMillis() < startTimestampMillis || event.getTimestampMillis() > endTimestampMillis) continue;
                            if (!MulticloudjUtils.matchesMetadata(event.getMetadata(), metadataQuery)) continue;
                            if (!MulticloudjUtils.matchesDimensions(event.getDimensions(), dimensionsQuery)) continue;
                            Double val = event.getDimensions().get(dimensionKey);
                            if (val != null) {
                                results.add(new Event(event.getTimestampMillis(), Collections.emptyMap(),
                                        Collections.singletonMap(dimensionKey, val)));
                            }
                        }
                    } catch (IOException e) {
                        hasFailed.set(true);
                        logger.warn("Exception on dimension call for key '{}': {}", key, e.getMessage());
                    }
                }, uploadPool));
            }
        }

        for (CompletableFuture<Void> f : futures) {
            try {
                f.get(60, TimeUnit.SECONDS);
            } catch (ExecutionException | TimeoutException e) {
                hasFailed.set(true);
            }
        }

        if (hasFailed.get()) {
            throw new IOException("exception on dimension call to blob storage");
        }
        return new ArrayList<>(results);
    }

    private void doExpire(final String namespace, final long endTimestampMillis) throws IOException {
        SimpleDateFormat minFormatter = new SimpleDateFormat(DIRECTORY_FORMATTER_MIN_PATTERN);
        minFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        List<String> allKeys = MulticloudjUtils.listKeys(bucketClient, getObjectKeyPrefix(namespace));
        List<String> toDelete = new ArrayList<>();

        for (String key : allKeys) {
            if (key.endsWith(NAMESPACE_IDENTIFIER)) continue;
            long keyTimestamp = extractTimestampFromKey(key, namespace);
            if (keyTimestamp >= 0 && keyTimestamp < endTimestampMillis) {
                toDelete.add(key);
            }
        }

        MulticloudjUtils.deleteBatched(bucketClient, toDelete);
    }

    private long extractTimestampFromKey(final String key, final String namespace) {
        try {
            String prefix = getObjectKeyPrefix(namespace) + "/";
            if (!key.startsWith(prefix)) return -1;
            String afterPrefix = key.substring(prefix.length());
            // afterPrefix looks like: yyyy/MM/dd/HH/mm.<cycle>.json or .b64
            if (afterPrefix.length() < 16) return -1;
            String timePart = afterPrefix.substring(0, 16); // yyyy/MM/dd/HH/mm
            SimpleDateFormat fmt = new SimpleDateFormat(DIRECTORY_FORMATTER_MIN_PATTERN);
            fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date parsed = fmt.parse(timePart);
            return parsed.getTime();
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * Validate and canonicalize the user-supplied buffer directory path.
     *
     * <p>F3 (CWE-22 / path traversal) guard. We reject any input that contains
     * a {@code ..} segment outright — even after normalization, a caller passing
     * {@code "/tmp/../etc/passwd"} is exhibiting traversal intent. We also reject
     * inputs that resolve to an existing non-directory (regular file, broken
     * symlink, etc.) so the constructor cannot be tricked into using a sensitive
     * location as a "buffer dir".
     */
    private static Path validateAndCanonicalizeBufferDirectory(final String bufferDirectory) {
        // Refuse traversal-style inputs without trying to be clever about resolving them.
        // Any segment equal to ".." is rejected, irrespective of whether the resulting
        // canonical path would stay inside an "expected base" - the project has no
        // standardized base, so the safest rule is "no .. in input".
        final Path raw;
        try {
            raw = Paths.get(bufferDirectory);
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("invalid buffer directory path: " + bufferDirectory, e);
        }
        for (Path segment : raw) {
            if ("..".equals(segment.toString())) {
                throw new IllegalArgumentException(
                        "buffer directory path must not contain '..' segments: " + bufferDirectory);
            }
        }
        final Path canonical = raw.normalize().toAbsolutePath();
        if (Files.exists(canonical) && !Files.isDirectory(canonical)) {
            throw new IllegalArgumentException(
                    "buffer directory path exists but is not a directory: " + canonical);
        }
        return canonical;
    }

    /**
     * Create the per-instance buffer directory with owner-only (700) permissions on POSIX
     * systems. F2 guard (CWE-276/200): buffered events may contain PII; world-readable
     * directories leak that data to any local user.
     *
     * <p>On non-POSIX file systems (e.g. Windows) the method falls back to {@code File}
     * read-permission tweaks, which is the strongest portable approximation Java offers.
     */
    private static void createBufferRootWithRestrictivePermissions(final Path bufferRoot) throws IOException {
        final boolean posix = FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
        if (posix) {
            // Create any missing parents first - createDirectories(path, attrs) applies the attrs
            // to every directory it creates, which on a multi-level parent tree could clobber an
            // existing parent's permissions. Build parents without attrs, then create the leaf
            // with 700.
            final Path parent = bufferRoot.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            if (!Files.exists(bufferRoot)) {
                Files.createDirectory(bufferRoot,
                        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
            } else {
                Files.setPosixFilePermissions(bufferRoot,
                        PosixFilePermissions.fromString("rwx------"));
            }
        } else {
            Files.createDirectories(bufferRoot);
            // Best-effort on non-POSIX (Windows): strip world-readable bit, then re-grant owner.
            File f = bufferRoot.toFile();
            //noinspection ResultOfMethodCallIgnored
            f.setReadable(false, false);
            //noinspection ResultOfMethodCallIgnored
            f.setReadable(true, true);
            //noinspection ResultOfMethodCallIgnored
            f.setWritable(false, false);
            //noinspection ResultOfMethodCallIgnored
            f.setWritable(true, true);
            //noinspection ResultOfMethodCallIgnored
            f.setExecutable(false, false);
            //noinspection ResultOfMethodCallIgnored
            f.setExecutable(true, true);
        }
    }

    private static void deleteDirectoryRecursively(final Path dir) {
        try {
            Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path d, IOException exc) throws IOException {
                    Files.delete(d);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            logger.warn("Failed to delete directory '{}': {}", dir, e.getMessage());
        }
    }
}
