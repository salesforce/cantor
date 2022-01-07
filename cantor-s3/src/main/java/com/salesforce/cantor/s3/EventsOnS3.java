package com.salesforce.cantor.s3;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.sift.MDCBasedDiscriminator;
import ch.qos.logback.classic.sift.SiftingAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.util.Duration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.google.common.cache.*;
import com.google.common.util.concurrent.*;
import com.google.gson.*;
import com.salesforce.cantor.Events;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.salesforce.cantor.common.EventsPreconditions.*;

public class EventsOnS3 extends AbstractBaseS3Namespaceable implements Events {
    private static final Logger logger = LoggerFactory.getLogger(EventsOnS3.class);

    private static final String defaultBufferDirectory = "cantor-events-s3-buffer";
    private static final long defaultFlushIntervalSeconds = 60;
    private static final long defaultTimeoutSeconds = 60;

    private static final String dimensionKeyPayloadOffset = ".cantor-payload-offset";
    private static final String dimensionKeyPayloadLength = ".cantor-payload-length";

    // parameter used for path to file for the sifting logger
    private static final String siftingDiscriminatorKey = "path";
    private static final Logger siftingLogger = initSiftingLogger();

    // cantor-events-<namespace>/<startTimestamp>-<endTimestamp>
    private static final String objectKeyPrefix = "cantor-events";

    // date formatter patterns for converting an event timestamp to a hierarchical directory structure
    private static final String directoryFormatterMinPattern = "yyyy/MM/dd/HH/mm";
    private static final String directoryFormatterHourPattern = "yyyy/MM/dd/HH/";
    private static final String cycleNameFormatterPattern = "yyyy-MM-dd_HH-mm-ss";

    // monitors for synchronizing writes to namespaces
    private static final Map<String, Object> namespaceLocks = new ConcurrentHashMap<>();

    // json parser
    private final Gson parser = new GsonBuilder().create();

    // reference to the flush cycle guid
    private final AtomicReference<String> currentFlushCycleGuid = new AtomicReference<>();

    // aws transfer manager for uploading buffer files
    private final TransferManager s3TransferManager;

    // path to directory to store buffered event logs
    private final String bufferDirectory;

    private final LoadingCache<String, AtomicLong> payloadOffset = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, AtomicLong>() {
                @Override
                public AtomicLong load(final String ignoredPath) {
                    return new AtomicLong(0);
                }
            });

    public EventsOnS3(final AmazonS3 s3Client,
                      final String bucketName) throws IOException {
        this(s3Client, bucketName, defaultBufferDirectory, defaultFlushIntervalSeconds);
    }

    public EventsOnS3(final AmazonS3 s3Client,
                      final String bucketName,
                      final String bufferDirectory) throws IOException {
        this(s3Client, bucketName, bufferDirectory, defaultFlushIntervalSeconds);
    }

    public EventsOnS3(final AmazonS3 s3Client,
                      final String bucketName,
                      final String bufferDirectory,
                      final long flushIntervalSeconds) throws IOException {
        super(s3Client, bucketName, "events");
        checkArgument(flushIntervalSeconds > 0, "invalid flush interval");
        checkString(bucketName, "invalid bucket name");
        checkString(bufferDirectory, "invalid buffer directory");

        this.bufferDirectory = bufferDirectory;

        // initialize s3 transfer manager
        final TransferManagerBuilder builder = TransferManagerBuilder.standard();
        builder.withS3Client(this.s3Client)
                .withMultipartUploadThreshold(32L * 1024 * 1024) // 32MB
                .withMinimumUploadPartSize(32L * 1024 * 1024) // 32MB
                .withExecutorFactory(() -> Executors.newFixedThreadPool(
                        32, // 32 concurrent threads to upload buffer files
                        new ThreadFactoryBuilder()
                                .setNameFormat("cantor-s3-event-transfer-manager-worker-%d")
                                .build()
                ));
        this.s3TransferManager = builder.build();

        // schedule flush cycle to start immediately
        rollover();
        // scheduler for flushing buffers
        Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("cantor-s3-buffer-flusher-" + trim(bucketName)).build()
        ).scheduleAtFixedRate(this::flush, 0, flushIntervalSeconds, TimeUnit.SECONDS);
    }

    @Override
    public void store(final String namespace, final Collection<Event> batch) throws IOException {
        checkStore(namespace, batch);
        checkNamespace(namespace);
        try {
            doStore(namespace, batch);
        } catch (final AmazonS3Exception e) {
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
        } catch (final AmazonS3Exception | InterruptedException e) {
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
            return doMetadata(namespace,
                    metadataKey,
                    startTimestampMillis,
                    endTimestampMillis,
                    (metadataQuery != null) ? metadataQuery : Collections.emptyMap(),
                    (dimensionsQuery != null) ? dimensionsQuery : Collections.emptyMap());
        } catch (final AmazonS3Exception | InterruptedException e) {
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
            return doDimension(namespace,
                    dimensionKey,
                    startTimestampMillis,
                    endTimestampMillis,
                    (metadataQuery != null) ? metadataQuery : Collections.emptyMap(),
                    (dimensionsQuery != null) ? dimensionsQuery : Collections.emptyMap());
        } catch (final AmazonS3Exception | InterruptedException e) {
            logger.warn("exception getting dimension from namespace: " + namespace, e);
            throw new IOException("exception getting dimension from namespace: " + namespace, e);
        }
    }

    @Override
    public void expire(final String namespace, final long endTimestampMillis) throws IOException {
        checkExpire(namespace, endTimestampMillis);
        checkNamespace(namespace);
        try {
            doExpire(namespace, endTimestampMillis);
        } catch (final AmazonS3Exception | InterruptedException e) {
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
        final String loggerName = "cantor-s3-events-sifting-logger";
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

    // storing each event in a json lines format to conform to s3 selects preferred format,
    // and payloads encoded in base64 in a separate file
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

        final String currentCycleName = getRolloverCycleName();
        final String cyclePath = getPath(currentCycleName);
        final String filePath = String.format("%s/%s/%s.%s",
                cyclePath, getObjectKeyPrefix(namespace), directoryFormatterMin.format(event.getTimestampMillis()), currentCycleName
        );
        final String payloadFilePath = filePath + ".b64";
        final String eventsFilePath = filePath + ".json";

        // make sure there is a lock object for this namespace
        namespaceLocks.putIfAbsent(namespace, namespace);

        synchronized (namespaceLocks.get(namespace)) {
            if (payload != null && payload.length > 0) {
                final String payloadBase64 = Base64.getEncoder().encodeToString(payload);
                append(payloadFilePath, payloadBase64);

                // one for new line at the end of the base64 encoded byte array
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
                              final int limit) throws IOException, InterruptedException {

        final List<Event> results = new CopyOnWriteArrayList<>();
        // parallel calls to s3
        final ListeningExecutorService executorService = newListeningExecutor("cantor-events-s3-get-%d");
        final AtomicBoolean futureHasFailed = new AtomicBoolean(false);

        // iterate over all s3 objects that match this request
        for (final String objectKey : getMatchingKeys(namespace, startTimestampMillis, endTimestampMillis)) {
            // only query json files
            if (!objectKey.endsWith("json")) {
                continue;
            }
            ListenableFuture<List<Event>> future = executorService.submit(
                () -> doGetOnObject(objectKey, startTimestampMillis, endTimestampMillis, metadataQuery,
                    dimensionsQuery, includePayloads)
            );
            FutureCallback<List<Event>> callback = new FutureCallback<List<Event>>() {
                // we want this handler to run immediately after we push the big red button!
                public void onSuccess(List<Event> events) {
                    results.addAll(events);
                }
                public void onFailure(Throwable e) {
                    futureHasFailed.set(true);
                    logger.warn("exception on get call to s3: {}", e.getMessage(), e);
                }
            };
            Futures.addCallback(future, callback, MoreExecutors.directExecutor());
        }

        awaitTermination(executorService);

        if (futureHasFailed.get()) {
            throw new IOException("exception on get call to s3");
        }

        // events are fetched from multiple sources, sort before returning
        sortEventsByTimestamp(results, ascending);
        if (limit > 0) {
            return results.subList(0, Math.min(limit, results.size()));
        }
        return results;
    }

    private Set<String> doMetadata(final String namespace,
                                   final String metadataKey,
                                   final long startTimestampMillis,
                                   final long endTimestampMillis,
                                   final Map<String, String> metadataQuery,
                                   final Map<String, String> dimensionsQuery) throws IOException, InterruptedException {

        final Set<String> results = new CopyOnWriteArraySet<>();
        // parallel calls to s3
        final ListeningExecutorService executorService = newListeningExecutor("cantor-events-s3-metadata-%d");
        final AtomicBoolean futureHasFailed = new AtomicBoolean(false);

        // iterate over all s3 objects that match this request
        for (final String objectKey : getMatchingKeys(namespace, startTimestampMillis, endTimestampMillis)) {
            // only query json files
            if (!objectKey.endsWith("json")) {
                continue;
            }
            ListenableFuture<Set<String>> future = executorService.submit(
                () -> doMetadataOnObject(objectKey, metadataKey, startTimestampMillis, endTimestampMillis,
                    metadataQuery, dimensionsQuery)
            );
            FutureCallback<Set<String>> callback = new FutureCallback<Set<String>>() {
                public void onSuccess(Set<String> metadata) {
                    results.addAll(metadata);
                }
                public void onFailure(Throwable e) {
                    futureHasFailed.set(true);
                    logger.warn("exception on metadata call to s3", e);
                }
            };
            Futures.addCallback(future, callback, MoreExecutors.directExecutor());
        }

        awaitTermination(executorService);

        if (futureHasFailed.get()) {
            throw new IOException("exception on metadata call to s3");
        }
        return results;
    }

    private List<Event> doDimension(final String namespace,
                                    final String dimensionKey,
                                    final long startTimestampMillis,
                                    final long endTimestampMillis,
                                    final Map<String, String> metadataQuery,
                                    final Map<String, String> dimensionsQuery) throws IOException, InterruptedException {
        final List<Event> results = new CopyOnWriteArrayList<>();
        // parallel calls to s3
        final ListeningExecutorService executorService = newListeningExecutor("cantor-events-s3-dimension-%d");
        final AtomicBoolean futureHasFailed = new AtomicBoolean(false);

        // iterate over all s3 objects that match this request
        for (final String objectKey : getMatchingKeys(namespace, startTimestampMillis, endTimestampMillis)) {
            // only query json files
            if (!objectKey.endsWith("json")) {
                continue;
            }
            ListenableFuture<List<Event>> future = executorService.submit(
                () -> doDimensionOnObject(objectKey, dimensionKey, startTimestampMillis, endTimestampMillis,
                    metadataQuery, dimensionsQuery)
            );
            FutureCallback<List<Event>> callback = new FutureCallback<List<Event>>() {
                public void onSuccess(List<Event> events) {
                    results.addAll(events);
                }
                public void onFailure(Throwable e) {
                    futureHasFailed.set(true);
                    logger.warn("exception on dimension call to s3", e);
                }
            };
            Futures.addCallback(future, callback, MoreExecutors.directExecutor());
        }

        awaitTermination(executorService);

        if (futureHasFailed.get()) {
            throw new IOException("exception on get call to s3");
        }
        return results;
    }

    private void sortEventsByTimestamp(final List<Event> events, final boolean ascending) {
        events.sort((event1, event2) -> {
            if (event1.getTimestampMillis() < event2.getTimestampMillis()) {
                return ascending ? -1 : 1;
            } else if (event1.getTimestampMillis() > event2.getTimestampMillis()) {
                return ascending ? 1 : -1;
            }
            return 0;
        });
    }

    private List<Event> doGetOnObject(final String objectKey,
                                      final long startTimestampMillis,
                                      final long endTimestampMillis,
                                      final Map<String, String> metadataQuery,
                                      final Map<String, String> dimensionsQuery,
                                      final boolean includePayloads) throws IOException {

        final List<Event> results = new ArrayList<>();
        final String query = generateGetQuery(startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        try (final Scanner lineReader = new Scanner(S3Utils.S3Select.queryObjectJson(this.s3Client, this.bucketName, objectKey, query))) {
            // json events are stored in json lines format, so one json object per line
            while (lineReader.hasNext()) {
                final Event event = this.parser.fromJson(lineReader.nextLine(), Event.class);
                // if include payloads is true, find the payload offset and length, do a range call to s3 to pull
                // the base64 representation of the payload bytes
                if (includePayloads
                        && event.getDimensions().containsKey(dimensionKeyPayloadOffset)
                        && event.getDimensions().containsKey(dimensionKeyPayloadLength)) {
                    final long offset = event.getDimensions().get(dimensionKeyPayloadOffset).longValue();
                    final long length = event.getDimensions().get(dimensionKeyPayloadLength).longValue();
                    final String payloadFilename = objectKey.substring(0, objectKey.lastIndexOf("json")) + "b64";
                    final byte[] payloadBase64Bytes = S3Utils.getObjectBytes(this.s3Client, this.bucketName, payloadFilename, offset, offset + length - 1);
                    if (payloadBase64Bytes.length == 0) {
                        throw new IOException("failed to retrieve payload for event");
                    }
                    final byte[] payload = Base64.getDecoder().decode(new String(payloadBase64Bytes));
                    final Event eventWithPayload = new Event(event.getTimestampMillis(), event.getMetadata(), event.getDimensions(), payload);
                    results.add(eventWithPayload);
                } else {
                    results.add(event);
                }
            }
        }
        return results;
    }

    private Set<String> doMetadataOnObject(final String objectKey,
                                           final String metadataKey,
                                           final long startTimestampMillis,
                                           final long endTimestampMillis,
                                           final Map<String, String> metadataQuery,
                                           final Map<String, String> dimensionsQuery) throws IOException {

        final Set<String> results = new HashSet<>();
        final String query = generateMetadataQuery(metadataKey, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        try (final Scanner lineReader = new Scanner(S3Utils.S3Select.queryObjectJson(this.s3Client, this.bucketName, objectKey, query))) {
            // json events are stored in json lines format, so one json object per line
            while (lineReader.hasNext()) {
                final Map<String, String> metadata = this.parser.fromJson(lineReader.nextLine(), Map.class);
                if (metadata.containsKey(metadataKey)) {
                    results.add(metadata.get(metadataKey));
                }
            }
        }
        return results;
    }

    private List<Event> doDimensionOnObject(final String objectKey,
                                            final String dimensionKey,
                                            final long startTimestampMillis,
                                            final long endTimestampMillis,
                                            final Map<String, String> metadataQuery,
                                            final Map<String, String> dimensionsQuery) throws IOException {
        final List<Event> results = new ArrayList<>();
        final String query = generateDimensionQuery(dimensionKey, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        try (final Scanner lineReader = new Scanner(S3Utils.S3Select.queryObjectJson(this.s3Client, this.bucketName, objectKey, query))) {
            // json events are stored in json lines format, so one json object per line
            while (lineReader.hasNext()) {
                final Map<String, Double> parsedJson = this.parser.fromJson(lineReader.nextLine(), Map.class);
                final long timestamp = parsedJson.get("timestampMillis").longValue();
                final Map<String, Double> dimensions = Collections.singletonMap(dimensionKey, parsedJson.get(dimensionKey));
                results.add(new Event(timestamp, Collections.emptyMap(), dimensions));
            }
        }
        return results;
    }

    private void doExpire(final String namespace, final long endTimestampMillis) throws IOException, InterruptedException {
        // TODO this has to be implemented properly
        logger.info("expiring namespace '{}' with end timestamp of '{}'", namespace, endTimestampMillis);
        final Set<String> keys = getMatchingKeys(namespace, 0, endTimestampMillis);
        logger.info("expiring objects: {}", keys);
        S3Utils.deleteObjects(this.s3Client, this.bucketName, keys);
    }

    // creates an s3 select compatible query
    // see https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-select.html
    private String generateGetQuery(final long startTimestampMillis,
                                 final long endTmestampMillis,
                                 final Map<String, String> metadataQuery,
                                 final Map<String, String> dimensionsQuery) {
        final String timestampClause = String.format("s.timestampMillis >= %d AND s.timestampMillis <= %d", startTimestampMillis, endTmestampMillis);
        return String.format("SELECT * FROM s3object[*] s WHERE %s %s %s",
                timestampClause,
                getMetadataQuerySql(metadataQuery),
                getDimensionsQuerySql(dimensionsQuery)
        );
    }

    private String generateMetadataQuery(final String metadataKey,
                                         final long startTimestampMillis,
                                         final long endTmestampMillis,
                                         final Map<String, String> metadataQuery,
                                         final Map<String, String> dimensionsQuery) {
        final String timestampClause = String.format("s.timestampMillis >= %d AND s.timestampMillis <= %d", startTimestampMillis, endTmestampMillis);
        return String.format("SELECT s.metadata.\"%s\" FROM s3object[*] s WHERE %s %s %s",
                metadataKey,
                timestampClause,
                getMetadataQuerySql(metadataQuery),
                getDimensionsQuerySql(dimensionsQuery)
        );
    }

    private String generateDimensionQuery(final String dimensionKey,
                                          final long startTimestampMillis,
                                          final long endTmestampMillis,
                                          final Map<String, String> metadataQuery,
                                          final Map<String, String> dimensionsQuery) {
        final String timestampClause = String.format("s.timestampMillis >= %d AND s.timestampMillis <= %d", startTimestampMillis, endTmestampMillis);
        return String.format("SELECT s.timestampMillis, s.dimensions.\"%s\" FROM s3object[*] s WHERE %s %s %s",
                dimensionKey,
                timestampClause,
                getMetadataQuerySql(metadataQuery),
                getDimensionsQuerySql(dimensionsQuery)
        );
    }

    private Set<String> getMatchingKeys(final String namespace, final long startTimestampMillis, final long endTimestampMillis)
            throws IOException {
        final DateFormat directoryFormatterMin = new SimpleDateFormat(directoryFormatterMinPattern);
        final DateFormat directoryFormatterHour = new SimpleDateFormat(directoryFormatterHourPattern);
        final Set<String> prefixes = new HashSet<>();
        long start = startTimestampMillis;
        while (start <= endTimestampMillis) {
            if (start + TimeUnit.HOURS.toMillis(1) <= endTimestampMillis) {
                prefixes.add(String.format("%s/%s", getObjectKeyPrefix(namespace), directoryFormatterHour.format(start)));
                // round to the nearest hour and then add one hour
                start = start / (60 * 60 * 1000) * (60 * 60 * 1000);
                start += TimeUnit.HOURS.toMillis(1);
            } else {
                prefixes.add(String.format("%s/%s", getObjectKeyPrefix(namespace), directoryFormatterMin.format(start)));
                start += TimeUnit.MINUTES.toMillis(1);
            }
        }
        prefixes.add(String.format("%s/%s", getObjectKeyPrefix(namespace), directoryFormatterMin.format(endTimestampMillis)));

        final Set<String> matchingKeys = new ConcurrentSkipListSet<>();
        final ListeningExecutorService executorService = newListeningExecutor("cantor-events-s3-get-matching-keys-%d");
        final AtomicBoolean futureHasFailed = new AtomicBoolean(false);

        for (final String prefix : prefixes) {
            ListenableFuture<Collection<String>> future = executorService.submit(
                () -> S3Utils.getKeys(this.s3Client, this.bucketName, prefix)
            );
            FutureCallback<Collection<String>> callback = new FutureCallback<Collection<String>>() {
                public void onSuccess(Collection<String> keys) {
                    matchingKeys.addAll(keys);
                }
                public void onFailure(Throwable e) {
                    futureHasFailed.set(true);
                    logger.warn("exception on getting object keys from s3", e);
                }
            };
            Futures.addCallback(future, callback, MoreExecutors.directExecutor());
        }

        awaitTermination(executorService);

        if (futureHasFailed.get()) {
            throw new IOException("exception on getting object keys from s3");
        }
        return matchingKeys;
    }

    // the metadata query object can contain these patterns:
    // '' (just a string): equals - 'user-id' => 'user-1'
    // '=': equals - 'user-id' => '=user-1'
    // '!=': not equals - 'user-id' => '!=user-1'
    // '~': limited regex like - 'user-id' => '~user-*'
    // '!~': inverted limited regex like - 'user-id' => '!~user-*'
    private String getMetadataQuerySql(final Map<String, String> metadataQuery) {
        if (metadataQuery.isEmpty()) {
            return "";
        }
        final StringBuilder sql = new StringBuilder();
        for (final Map.Entry<String, String> entry : metadataQuery.entrySet()) {
            final String metadataName = prefixMetadata(entry.getKey());
            final String query = entry.getValue();
            // s3 select only supports limited regex
            if (query.startsWith("~")) {
                sql.append(" AND ").append(metadataName).append(" LIKE ").append(quote(regexToSql(query.substring(1))));
            } else if (query.startsWith("!~")) {
                sql.append(" AND ").append(metadataName).append(" NOT LIKE ").append(quote(regexToSql(query.substring(2))));
            } else if (query.startsWith("=")) {
                sql.append(" AND ").append(metadataName).append("=").append(quote(query.substring(1)));
            } else if (query.startsWith("!=")) {
                sql.append(" AND ").append(metadataName).append("!=").append(quote(query.substring(2)));
            } else {
                sql.append(" AND ").append(metadataName).append("=").append(quote(query));
            }
        }
        return sql.toString();
    }

    private String regexToSql(final String regex) {
        return regex
                .replace("*", "%")
                .replace("_", "\\\\_");
    }

    // the dimension query object can contain these patterns:
    // '' (just a number): equals - 'cpu' => '90'
    // '=': equals - 'cpu' => '=90'
    // '!=': not equals - 'cpu' => '!=90'
    // '..': between - 'cpu' => '90..100'
    // '>': greater than - 'cpu' => '>90'
    // '>=': greater than or equals - 'cpu' => '>=90'
    // '<': less than - 'cpu' => '<90'
    // '<=': less than or equals - 'cpu' => '<=90'
    private String getDimensionsQuerySql(final Map<String, String> dimensionsQuery) {
        if (dimensionsQuery.isEmpty()) {
            return "";
        }
        final StringBuilder sql = new StringBuilder();
        for (final Map.Entry<String, String> entry : dimensionsQuery.entrySet()) {
            final String dimensionName = prefixDimension(entry.getKey());
            final String query = entry.getValue();
            if (query.contains("..")) {
                sql.append(" AND ")
                    .append(dimensionName)
                    .append(" BETWEEN ")
                    .append(Double.valueOf(query.substring(0, query.indexOf(".."))))
                    .append(" AND ")
                    .append(Double.valueOf(query.substring(query.indexOf("..") + 2)));
            } else if (query.startsWith(">=")) {
                sql.append(" AND ").append(dimensionName).append(">=").append(query.substring(2));
            } else if (query.startsWith("<=")) {
                sql.append(" AND ").append(dimensionName).append("<=").append(query.substring(2));
            } else if (query.startsWith(">")) {
                sql.append(" AND ").append(dimensionName).append(">").append(query.substring(1));
            } else if (query.startsWith("<")) {
                sql.append(" AND ").append(dimensionName).append("<").append(query.substring(1));
            } else if (query.startsWith("!=")) {
                sql.append(" AND ").append(dimensionName).append("!=").append(query.substring(2));
            } else if (query.startsWith("=")) {
                sql.append(" AND ").append(dimensionName).append("=").append(query.substring(1));
            } else {
                sql.append(" AND ").append(dimensionName).append("=").append(query);
            }
        }
        return sql.toString();
    }

    private String quote(final String key) {
        return String.format("'%s'", key);
    }

    private String prefixMetadata(final String key) {
        return String.format("s.metadata.\"%s\"", key);
    }

    private String prefixDimension(final String key) {
        return String.format("CAST ( s.dimensions.\"%s\" as decimal)", key);
    }

    // update the rollover cycle guid and return the previous one
    private void rollover() {
        // date directoryFormatter for flush cycle name calculation
        final DateFormat cycleNameFormatter = new SimpleDateFormat(cycleNameFormatterPattern);
        // cycle name is: <timestamp>-<guid>
        final String rolloverCycleName = String.format("%s.%s",
                cycleNameFormatter.format(System.currentTimeMillis()),
                UUID.randomUUID().toString().replaceAll("-", "")
        );
        logger.info("starting new cycle: {}", rolloverCycleName);
        setRolloverCycleName(rolloverCycleName);
    }

    private String getRolloverCycleName() {
        return this.currentFlushCycleGuid.get();
    }

    private void setRolloverCycleName(final String name) {
        this.currentFlushCycleGuid.set(name);
    }

    private String getPath(final String rolloverCycleName) {
        return this.bufferDirectory + File.separator + rolloverCycleName;
    }

    private void flush() {
        final long startMillis = System.currentTimeMillis();
        try {
            rollover();

            final File bufferDirectoryFile = new File(this.bufferDirectory);
            if (!bufferDirectoryFile.exists() || !bufferDirectoryFile.canWrite() || !bufferDirectoryFile.isDirectory()) {
                logger.info("buffer directory '{}' does not exist or is not writable", this.bufferDirectory);
                return;
            }

            final List<File> toDelete = new ArrayList<>();
            for (final File toUpload : bufferDirectoryFile.listFiles()) {
                logger.info("uploading buffer directory: {}", toUpload.getAbsolutePath());
                // skip if path does not exist or is not a directory
                if (!toUpload.exists() || !toUpload.isDirectory()) {
                    logger.info("nothing to upload");
                    continue;
                }

                // upload all of the contents of the directory to s3
                uploadDirectory(toUpload);

                logger.info("successfully uploaded buffer directory: {}", toUpload.getAbsolutePath());
                toDelete.add(toUpload);
            }

            for (final File file : toDelete) {
                logger.info("deleting buffer file: {}", file.getAbsolutePath());
                delete(file);
            }
        } catch (InterruptedException e) {
            logger.warn("flush cycle interrupted; exiting");
        } catch (Exception e) {
            logger.warn("exception during flush", e);
        } finally {
            final long endMillis = System.currentTimeMillis();
            final long elapsedSeconds = (endMillis - startMillis) / 1_000;
            logger.info("flush cycle elapsed time: {}s", elapsedSeconds);
        }
    }

    private void uploadDirectory(final File toUpload) throws InterruptedException {
        final MultipleFileUpload upload = this.s3TransferManager.uploadDirectory(this.bucketName, null, toUpload, true, (file, metadata) -> {
            // set object content type to plain text
            metadata.setContentType("text/plain");
        }, uploadContext -> {
            // extract the object namespace key and attach it as a tag
            final String key = uploadContext.getKey();
            final String tag = key.substring(key.indexOf("/") + 1);
            return new ObjectTagging(Collections.singletonList(new Tag("namespace", tag.substring(0, tag.indexOf("/")))));
        },
        // ensure ownership is given to the bucket on store
        file -> CannedAccessControlList.BucketOwnerFullControl);
        // log the upload progress
        do {
            logger.info("s3 transfer progress of '{}': {}% of {}mb state: {}",
                    toUpload.getAbsolutePath(),
                    (int) upload.getProgress().getPercentTransferred(),
                    upload.getProgress().getTotalBytesToTransfer() / (1024*1024),
                    upload.getState()
            );
            Thread.sleep(1_000);
        } while (!upload.isDone());
        // waiting ensures we throw exception on any s3 errors during upload
        upload.waitForCompletion();
    }

    private void delete(final File dir) {
        final File[] files = dir.listFiles();
        if (files != null) {
            for (final File file : files) {
                delete(file);
            }
        }
        dir.delete();
    }

    private static ListeningExecutorService newListeningExecutor(final String nameFormat) {
        return MoreExecutors.listeningDecorator(
                Executors.newFixedThreadPool(32, new ThreadFactoryBuilder().setNameFormat(nameFormat).build())
        );
    }

    private static void awaitTermination(final ExecutorService executor) throws IOException {
        executor.shutdown();
        try {
            executor.awaitTermination(defaultTimeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
