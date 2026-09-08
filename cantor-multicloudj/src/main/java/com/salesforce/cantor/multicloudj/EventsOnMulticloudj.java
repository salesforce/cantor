/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.multicloudj;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.sift.MDCBasedDiscriminator;
import ch.qos.logback.classic.sift.SiftingAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.util.Duration;
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
import org.slf4j.MDC;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.common.CommonPreconditions.checkNamespace;
import static com.salesforce.cantor.common.CommonPreconditions.checkState;
import static com.salesforce.cantor.common.CommonPreconditions.checkString;
import static com.salesforce.cantor.common.EventsPreconditions.checkDimension;
import static com.salesforce.cantor.common.EventsPreconditions.checkExpire;
import static com.salesforce.cantor.common.EventsPreconditions.checkGet;
import static com.salesforce.cantor.common.EventsPreconditions.checkMetadata;
import static com.salesforce.cantor.common.EventsPreconditions.checkStore;

/**
 * Events backend over a multicloudj BucketClient with local buffering and scheduled flush.
 *
 * <p><b>Write path:</b> {@link #store(String, Collection)} appends each event as a JSON-line to a
 * local file in the buffer directory, keyed by namespace and event minute-bucket. Payloads, when
 * present, are base64-encoded and appended to a sibling {@code .b64} file; the event's
 * {@code .cantor-payload-offset} and {@code .cantor-payload-length} dimensions point back into it.
 *
 * <p><b>Flush:</b> a single scheduled thread per bucket wakes up every {@code flushIntervalSeconds}
 * (default 60), rolls the cycle GUID, waits 3 seconds for in-flight writes to settle, then uploads
 * each non-current cycle directory via {@link MulticloudjUtils#uploadDirectory}, tagging each
 * uploaded object with its namespace.
 *
 * <p><b>Queries:</b> {@code get/metadata/dimension} download every matching object and apply
 * filters in this process. There is no server-side query pushdown.
 *
 * <p>Behavior mirrors {@code com.salesforce.cantor.s3.EventsOnS3}, with two intentional differences:
 * (1) per-object ACLs are not set (auth is configured on the {@link BucketClient}), and
 * (2) the AWS {@code TransferManager} is replaced by a manual parallel uploader.
 */
public class EventsOnMulticloudj extends AbstractBaseMulticloudjNamespaceable implements Events {
    private static final Logger logger = LoggerFactory.getLogger(EventsOnMulticloudj.class);

    private static final String defaultBufferDirectory = "cantor-events-multicloudj-buffer";
    private static final long defaultFlushIntervalSeconds = 60;
    private static final int uploadParallelism = 32;

    private static final String dimensionKeyPayloadOffset = ".cantor-payload-offset";
    private static final String dimensionKeyPayloadLength = ".cantor-payload-length";

    // parameter used for path to file for the sifting logger
    private static final String siftingDiscriminatorKey = "path";
    private static final Logger siftingLogger = initSiftingLogger();

    // cantor-events/<trimmed-namespace>/yyyy/MM/dd/HH/mm.<cycle>.{json,b64}
    private static final String objectKeyPrefix = "cantor-events";
    private static final String directoryFormatterMinPattern = "yyyy/MM/dd/HH/mm";
    private static final String directoryFormatterHourPattern = "yyyy/MM/dd/HH/";
    private static final String cycleNameFormatterPattern = "yyyy-MM-dd_HH-mm-ss";

    // monitors for synchronizing writes to namespaces (shared across instances)
    private static final Map<String, Object> namespaceLocks = new ConcurrentHashMap<>();

    // one flush thread per bucket name (shared across instances pointing at the same bucket)
    private static final Map<String, ScheduledExecutorService> flushExecutorServices = new ConcurrentHashMap<>();

    private final Gson parser = new GsonBuilder().create();

    private final AtomicReference<String> currentFlushCycleGuid = new AtomicReference<>();

    // path to directory to store buffered event logs
    private final String bufferDirectory;

    // running offset within each .b64 payload file
    private final LoadingCache<String, AtomicLong> payloadOffset = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, AtomicLong>() {
                @Override
                public AtomicLong load(final String ignoredPath) {
                    return new AtomicLong(0);
                }
            });

    public EventsOnMulticloudj(final BucketClient bucketClient) throws IOException {
        this(bucketClient, defaultBufferDirectory, defaultFlushIntervalSeconds);
    }

    public EventsOnMulticloudj(final BucketClient bucketClient, final String bufferDirectory) throws IOException {
        this(bucketClient, bufferDirectory, defaultFlushIntervalSeconds);
    }

    public EventsOnMulticloudj(final BucketClient bucketClient,
                               final String bufferDirectory,
                               final long flushIntervalSeconds) throws IOException {
        super(bucketClient);
        checkArgument(flushIntervalSeconds > 0, "invalid flush interval");
        checkString(bufferDirectory, "invalid buffer directory");
        this.bufferDirectory = bufferDirectory;

        // start the first cycle immediately
        rollover();

        // one flush thread per bucket; computeIfAbsent ensures a single executor regardless of
        // how many EventsOnMulticloudj instances point at the same bucket
        flushExecutorServices.computeIfAbsent(bucketClient.getBucket(), unused -> {
            final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                    MulticloudjUtils.namedThreadFactory(
                            "cantor-multicloudj-buffer-flusher-" + bucketClient.getBucket() + "-%d")
            );
            scheduledExecutorService.scheduleWithFixedDelay(this::flush, 0, flushIntervalSeconds, TimeUnit.SECONDS);
            return scheduledExecutorService;
        });
    }

    @Override
    public void store(final String namespace, final Collection<Event> batch) throws IOException {
        checkStore(namespace, batch);
        checkNamespace(namespace);
        try {
            doStore(namespace, batch);
        } catch (final SubstrateSdkException e) {
            logger.warn("exception storing events to namespace: " + namespace, e);
            throw new IOException("exception storing events to namespace: " + namespace, e);
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
        checkNamespace(namespace);
        try {
            return doGet(namespace,
                    startTimestampMillis,
                    endTimestampMillis,
                    (metadataQuery != null) ? metadataQuery : Collections.emptyMap(),
                    (dimensionsQuery != null) ? dimensionsQuery : Collections.emptyMap(),
                    includePayloads,
                    ascending,
                    limit);
        } catch (final SubstrateSdkException e) {
            logger.warn("exception getting events from namespace: " + namespace, e);
            throw new IOException("exception getting events from namespace: " + namespace, e);
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
        checkNamespace(namespace);
        try {
            final Set<String> results = new HashSet<>();
            for (final Event event : doGet(namespace,
                    startTimestampMillis,
                    endTimestampMillis,
                    (metadataQuery != null) ? metadataQuery : Collections.emptyMap(),
                    (dimensionsQuery != null) ? dimensionsQuery : Collections.emptyMap(),
                    false, true, 0)) {
                final String value = event.getMetadata().get(metadataKey);
                if (value != null) {
                    results.add(value);
                }
            }
            return results;
        } catch (final SubstrateSdkException e) {
            logger.warn("exception getting metadata from namespace: " + namespace, e);
            throw new IOException("exception getting metadata from namespace: " + namespace, e);
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
        checkNamespace(namespace);
        try {
            final List<Event> results = new ArrayList<>();
            for (final Event event : doGet(namespace,
                    startTimestampMillis,
                    endTimestampMillis,
                    (metadataQuery != null) ? metadataQuery : Collections.emptyMap(),
                    (dimensionsQuery != null) ? dimensionsQuery : Collections.emptyMap(),
                    false, true, 0)) {
                final Double value = event.getDimensions().get(dimensionKey);
                if (value != null) {
                    results.add(new Event(
                            event.getTimestampMillis(),
                            Collections.emptyMap(),
                            Collections.singletonMap(dimensionKey, value)
                    ));
                }
            }
            return results;
        } catch (final SubstrateSdkException e) {
            logger.warn("exception getting dimension from namespace: " + namespace, e);
            throw new IOException("exception getting dimension from namespace: " + namespace, e);
        }
    }

    @Override
    public void expire(final String namespace, final long endTimestampMillis) throws IOException {
        checkExpire(namespace, endTimestampMillis);
        checkNamespace(namespace);
        try {
            logger.info("expiring namespace '{}' with end timestamp of '{}'", namespace, endTimestampMillis);
            final Set<String> keys = getMatchingKeys(namespace, 0, endTimestampMillis);
            logger.info("expiring objects: {}", keys);
            MulticloudjUtils.deleteObjects(this.bucketClient, keys);
        } catch (final SubstrateSdkException e) {
            logger.warn("exception expiring events from namespace: " + namespace, e);
            throw new IOException("exception expiring events from namespace: " + namespace, e);
        }
    }

    @Override
    protected String getObjectKeyPrefix(final String namespace) {
        return String.format("%s/%s", objectKeyPrefix, trim(namespace));
    }

    private static Logger initSiftingLogger() {
        final LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        final SiftingAppender siftingAppender = new SiftingAppender();
        final String loggerName = "cantor-multicloudj-events-sifting-logger";
        siftingAppender.setName(loggerName);
        siftingAppender.setContext(loggerContext);

        final MDCBasedDiscriminator discriminator = new MDCBasedDiscriminator();
        discriminator.setKey(siftingDiscriminatorKey);
        discriminator.setDefaultValue("unknown");
        discriminator.start();
        siftingAppender.setDiscriminator(discriminator);
        siftingAppender.setTimeout(Duration.buildBySeconds(3));
        siftingAppender.setAppenderFactory((context, discriminatingValue) -> {
            final FileAppender<ILoggingEvent> fileAppender = new FileAppender<>();
            fileAppender.setName("file-" + discriminatingValue);
            fileAppender.setContext(context);
            fileAppender.setFile(discriminatingValue);

            final PatternLayoutEncoder patternLayoutEncoder = new PatternLayoutEncoder();
            patternLayoutEncoder.setContext(context);
            patternLayoutEncoder.setPattern("%msg%n");
            patternLayoutEncoder.start();
            fileAppender.setEncoder(patternLayoutEncoder);

            fileAppender.start();
            return fileAppender;
        });
        siftingAppender.start();

        final ch.qos.logback.classic.Logger logger = loggerContext.getLogger(loggerName);
        logger.setAdditive(false);
        logger.setLevel(Level.ALL);
        logger.addAppender(siftingAppender);
        return logger;
    }

    // store each event as a JSON line; payloads (when present) are base64-encoded into a sibling .b64 file
    private void doStore(final String namespace, final Collection<Event> batch) {
        for (final Event event : batch) {
            appendEvent(namespace, event);
        }
    }

    private void appendEvent(final String namespace, final Event event) {
        final DateFormat directoryFormatterMin = new SimpleDateFormat(directoryFormatterMinPattern);
        final Map<String, String> metadata = new HashMap<>(event.getMetadata());
        final Map<String, Double> dimensions = new HashMap<>(event.getDimensions());
        final byte[] payload = event.getPayload();

        // ensure a lock object exists for this namespace
        namespaceLocks.putIfAbsent(namespace, namespace);

        synchronized (namespaceLocks.get(namespace)) {
            final String currentCycleName = getRolloverCycleName();
            final String cyclePath = getPath(currentCycleName);
            final String filePath = String.format("%s/%s/%s.%s",
                    cyclePath, getObjectKeyPrefix(namespace),
                    directoryFormatterMin.format(event.getTimestampMillis()), currentCycleName
            );
            final String payloadFilePath = filePath + ".b64";
            final String eventsFilePath = filePath + ".json";

            if (payload != null && payload.length > 0) {
                final String payloadBase64 = Base64.getEncoder().encodeToString(payload);
                append(payloadFilePath, payloadBase64);

                // +1 for the newline appended by the sifting logger
                final long offset = this.payloadOffset.getUnchecked(payloadFilePath).getAndAdd(payloadBase64.length() + 1);
                dimensions.put(dimensionKeyPayloadOffset, (double) offset);
                dimensions.put(dimensionKeyPayloadLength, (double) payloadBase64.length());
            }
            final Event toWrite = new Event(event.getTimestampMillis(), metadata, dimensions);
            append(eventsFilePath, this.parser.toJson(toWrite));
        }
    }

    private synchronized void append(final String path, final String message) {
        MDC.put(siftingDiscriminatorKey, path);
        siftingLogger.info(message);
        MDC.remove(siftingDiscriminatorKey);
    }

    private List<Event> doGet(final String namespace,
                              final long startTimestampMillis,
                              final long endTimestampMillis,
                              final Map<String, String> metadataQuery,
                              final Map<String, String> dimensionsQuery,
                              final boolean includePayloads,
                              final boolean ascending,
                              final int limit) throws IOException {
        final List<Event> results = new ArrayList<>();
        for (final String objectKey : getMatchingKeys(namespace, startTimestampMillis, endTimestampMillis)) {
            if (!objectKey.endsWith(".json")) {
                continue;
            }
            results.addAll(scanObject(objectKey, startTimestampMillis, endTimestampMillis,
                    metadataQuery, dimensionsQuery, includePayloads));
        }
        sortEventsByTimestamp(results, ascending);
        if (limit > 0 && results.size() > limit) {
            return new ArrayList<>(results.subList(0, limit));
        }
        return results;
    }

    private List<Event> scanObject(final String objectKey,
                                   final long startTimestampMillis,
                                   final long endTimestampMillis,
                                   final Map<String, String> metadataQuery,
                                   final Map<String, String> dimensionsQuery,
                                   final boolean includePayloads) throws IOException {
        final List<Event> results = new ArrayList<>();
        final byte[] objectBytes = MulticloudjUtils.getObjectBytes(this.bucketClient, objectKey);
        try (final Scanner lineReader = new Scanner(new String(objectBytes, StandardCharsets.UTF_8))) {
            while (lineReader.hasNext()) {
                final Event event = this.parser.fromJson(lineReader.nextLine(), Event.class);
                if (event.getTimestampMillis() < startTimestampMillis || event.getTimestampMillis() > endTimestampMillis) {
                    continue;
                }
                if (!matchesMetadata(event, metadataQuery)) {
                    continue;
                }
                if (!matchesDimensions(event, dimensionsQuery)) {
                    continue;
                }
                if (includePayloads
                        && event.getDimensions().containsKey(dimensionKeyPayloadOffset)
                        && event.getDimensions().containsKey(dimensionKeyPayloadLength)) {
                    final long offset = event.getDimensions().get(dimensionKeyPayloadOffset).longValue();
                    final long length = event.getDimensions().get(dimensionKeyPayloadLength).longValue();
                    final String payloadFilename = objectKey.substring(0, objectKey.lastIndexOf(".json")) + ".b64";
                    final byte[] payloadBase64Bytes = MulticloudjUtils.getObjectBytes(
                            this.bucketClient, payloadFilename, offset, offset + length - 1);
                    if (payloadBase64Bytes.length == 0) {
                        throw new IOException("failed to retrieve payload for event");
                    }
                    final byte[] payload = Base64.getDecoder().decode(new String(payloadBase64Bytes, StandardCharsets.UTF_8));
                    results.add(new Event(event.getTimestampMillis(), event.getMetadata(), event.getDimensions(), payload));
                } else {
                    results.add(event);
                }
            }
        }
        return results;
    }

    // metadata query syntax (mirrors EventsOnS3):
    //   ''       (just a string): equals
    //   '='      equals
    //   '!='     not equals
    //   '~'      limited regex like (* wildcard)
    //   '!~'     inverted limited regex like
    private static boolean matchesMetadata(final Event event, final Map<String, String> metadataQuery) {
        for (final Map.Entry<String, String> entry : metadataQuery.entrySet()) {
            final String actual = event.getMetadata().get(entry.getKey());
            if (actual == null) {
                return false;
            }
            final String query = entry.getValue();
            if (query.startsWith("!~")) {
                if (likeMatch(actual, query.substring(2))) return false;
            } else if (query.startsWith("~")) {
                if (!likeMatch(actual, query.substring(1))) return false;
            } else if (query.startsWith("!=")) {
                if (actual.equals(query.substring(2))) return false;
            } else if (query.startsWith("=")) {
                if (!actual.equals(query.substring(1))) return false;
            } else {
                if (!actual.equals(query)) return false;
            }
        }
        return true;
    }

    // dimension query syntax (mirrors EventsOnS3):
    //   ''      (just a number): equals
    //   '='     equals
    //   '!='    not equals
    //   '..'    between (inclusive)
    //   '>'     greater than
    //   '>='    greater than or equals
    //   '<'     less than
    //   '<='    less than or equals
    private static boolean matchesDimensions(final Event event, final Map<String, String> dimensionsQuery) {
        for (final Map.Entry<String, String> entry : dimensionsQuery.entrySet()) {
            final Double actual = event.getDimensions().get(entry.getKey());
            if (actual == null) {
                return false;
            }
            final String query = entry.getValue();
            if (query.contains("..")) {
                final double low = Double.parseDouble(query.substring(0, query.indexOf("..")));
                final double high = Double.parseDouble(query.substring(query.indexOf("..") + 2));
                if (actual < low || actual > high) return false;
            } else if (query.startsWith(">=")) {
                if (actual < Double.parseDouble(query.substring(2))) return false;
            } else if (query.startsWith("<=")) {
                if (actual > Double.parseDouble(query.substring(2))) return false;
            } else if (query.startsWith(">")) {
                if (actual <= Double.parseDouble(query.substring(1))) return false;
            } else if (query.startsWith("<")) {
                if (actual >= Double.parseDouble(query.substring(1))) return false;
            } else if (query.startsWith("!=")) {
                if (actual == Double.parseDouble(query.substring(2))) return false;
            } else if (query.startsWith("=")) {
                if (actual != Double.parseDouble(query.substring(1))) return false;
            } else {
                if (actual != Double.parseDouble(query)) return false;
            }
        }
        return true;
    }

    // SQL-LIKE-style match: '*' is wildcard, all other characters literal
    private static boolean likeMatch(final String actual, final String pattern) {
        final StringBuilder regex = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            final char c = pattern.charAt(i);
            if (c == '*') {
                regex.append(".*");
            } else {
                regex.append(Pattern.quote(String.valueOf(c)));
            }
        }
        return actual.matches(regex.toString());
    }

    private static void sortEventsByTimestamp(final List<Event> events, final boolean ascending) {
        events.sort((a, b) -> {
            if (a.getTimestampMillis() < b.getTimestampMillis()) return ascending ? -1 : 1;
            if (a.getTimestampMillis() > b.getTimestampMillis()) return ascending ? 1 : -1;
            return 0;
        });
    }

    private Set<String> getMatchingKeys(final String namespace, final long startTimestampMillis, final long endTimestampMillis) {
        final DateFormat directoryFormatterMin = new SimpleDateFormat(directoryFormatterMinPattern);
        final DateFormat directoryFormatterHour = new SimpleDateFormat(directoryFormatterHourPattern);
        final Set<String> prefixes = new HashSet<>();
        long start = startTimestampMillis;
        while (start <= endTimestampMillis) {
            if (start + TimeUnit.HOURS.toMillis(1) <= endTimestampMillis) {
                prefixes.add(String.format("%s/%s", getObjectKeyPrefix(namespace), directoryFormatterHour.format(start)));
                start = start / (60 * 60 * 1000) * (60 * 60 * 1000);
                start += TimeUnit.HOURS.toMillis(1);
            } else {
                prefixes.add(String.format("%s/%s", getObjectKeyPrefix(namespace), directoryFormatterMin.format(start)));
                start += TimeUnit.MINUTES.toMillis(1);
            }
        }
        prefixes.add(String.format("%s/%s", getObjectKeyPrefix(namespace), directoryFormatterMin.format(endTimestampMillis)));

        final Set<String> matchingKeys = new HashSet<>();
        for (final String prefix : prefixes) {
            matchingKeys.addAll(MulticloudjUtils.getKeys(this.bucketClient, prefix));
        }
        return matchingKeys;
    }

    private void rollover() {
        final DateFormat cycleNameFormatter = new SimpleDateFormat(cycleNameFormatterPattern);
        final String rolloverCycleName = String.format("%s.%s",
                cycleNameFormatter.format(System.currentTimeMillis()),
                UUID.randomUUID().toString().replaceAll("-", "")
        );
        logger.info("starting new cycle: {}", rolloverCycleName);
        this.currentFlushCycleGuid.set(rolloverCycleName);
    }

    private String getRolloverCycleName() {
        return this.currentFlushCycleGuid.get();
    }

    private String getPath(final String rolloverCycleName) {
        return this.bufferDirectory + File.separator + rolloverCycleName;
    }

    /**
     * Test hook: synchronously runs one flush cycle. Package-private so tests can force a
     * deterministic upload without waiting on the scheduled executor.
     */
    void flushNow() {
        flush();
    }

    private void flush() {
        final long startMillis = System.currentTimeMillis();
        try {
            rollover();

            // wait for in-flight writes to the previous cycle to complete
            Thread.sleep(3_000);

            final File bufferDirectoryFile = new File(this.bufferDirectory);
            if (!bufferDirectoryFile.exists() || !bufferDirectoryFile.canWrite() || !bufferDirectoryFile.isDirectory()) {
                logger.info("buffer directory '{}' does not exist or is not writable", this.bufferDirectory);
                return;
            }

            final File[] toUploadDirs = bufferDirectoryFile.listFiles();
            checkState(toUploadDirs != null, "list of buffer directories to upload is null");
            logger.info("total number of directories to upload: {}", toUploadDirs.length);

            for (final File dir : toUploadDirs) {
                logger.info("uploading buffer directory: {}", dir.getAbsolutePath());
                if (!dir.exists() || !dir.isDirectory()) {
                    logger.info("nothing to upload");
                    continue;
                }

                // skip the in-flight cycle directory
                if (dir.getName().contains(getRolloverCycleName())) {
                    continue;
                }

                MulticloudjUtils.uploadDirectory(this.bucketClient, dir, uploadParallelism, EventsOnMulticloudj::namespaceTagFor);
                logger.info("successfully uploaded buffer directory: {}", dir.getAbsolutePath());

                logger.info("deleting buffer directory: {}", dir.getAbsolutePath());
                deleteRecursively(dir);
            }
        } catch (final InterruptedException e) {
            logger.warn("flush cycle interrupted; exiting");
            Thread.currentThread().interrupt();
        } catch (final Exception e) {
            logger.warn("exception during flush", e);
        } finally {
            final long elapsedSeconds = (System.currentTimeMillis() - startMillis) / 1_000;
            logger.info("flush cycle elapsed time: {}s", elapsedSeconds);
        }
    }

    // upload key looks like "cantor-events/<ns-slug>/yyyy/MM/dd/HH/mm.<cycle>.json";
    // pull the namespace slug out for the object tag
    private static String namespaceTagFor(final String objectKey) {
        final int firstSlash = objectKey.indexOf('/');
        if (firstSlash < 0) {
            return null;
        }
        final String afterFirst = objectKey.substring(firstSlash + 1);
        final int secondSlash = afterFirst.indexOf('/');
        if (secondSlash < 0) {
            return afterFirst;
        }
        return afterFirst.substring(0, secondSlash);
    }

    private static void deleteRecursively(final File dir) {
        final File[] files = dir.listFiles();
        if (files != null) {
            for (final File file : files) {
                deleteRecursively(file);
            }
        }
        if (!dir.delete()) {
            logger.debug("failed to delete: {}", dir.getAbsolutePath());
        }
    }
}
