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
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.*;

import static com.salesforce.cantor.common.EventsPreconditions.*;

public class EventsOnS3 extends AbstractBaseS3Namespaceable implements Events {
    private static final Logger logger = LoggerFactory.getLogger(EventsOnS3.class);

    private static final String defaultBufferDirectory = "/tmp/cantor-events-s3-buffer";
    private static final long defaultFlushIntervalSeconds = 30;

    private static final String dimensionKeyPayloadOffset = ".payload-offset";
    private static final String dimensionKeyPayloadLength = ".payload-length";

    private final AtomicReference<String> currentFlushCycleGuid = new AtomicReference<>();
    private final Gson parser = new GsonBuilder().create();
    private final Logger siftingLogger = initSiftingLogger();

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final String bufferDirectory;
    private final long maxFlushIntervalSeconds;

    // cantor-events-<namespace>/<startTimestamp>-<endTimestamp>
    private static final String objectKeyPrefix = "cantor-events-%s/";

    // date directoryFormatter for flush cycle name calculation
    private final DateFormat cycleNameFormatter = new SimpleDateFormat("YYYY-MM-dd_HH-mm-ss");
    // date directoryFormatter for converting an event timestamp to a hierarchical directory structure
    private final DateFormat directoryFormatter = new SimpleDateFormat("YYYY/MM/dd/HH/mm");
    private final DateFormat directoryFormatterHourly = new SimpleDateFormat("YYYY/MM/dd/HH/");
    private final DateFormat directoryFormatterDayly = new SimpleDateFormat("YYYY/MM/dd/");
    private final DateFormat directoryFormatterMonthly = new SimpleDateFormat("YYYY/MM/");
    private final DateFormat directoryFormatterYearly = new SimpleDateFormat("YYYY/");
    private final TransferManager s3TransferManager;

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
                      final long maxFlushIntervalSeconds) throws IOException {
        super(s3Client, bucketName, "events");
        this.bufferDirectory = bufferDirectory;
        this.maxFlushIntervalSeconds = maxFlushIntervalSeconds;

        // initialize s3 transfer manager
        final TransferManagerBuilder builder = TransferManagerBuilder.standard();
        builder.setS3Client(this.s3Client);
        this.s3TransferManager = builder.build();

        // schedule flush cycle to start immediately
        this.executor.submit(this::flush);
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
        } catch (final AmazonS3Exception e) {
            logger.warn("exception getting events from namespace: " + namespace, e);
            throw new IOException("exception getting events from namespace: " + namespace, e);
        }
    }

    @Override
    public int delete(final String namespace,
                      final long startTimestampMillis,
                      final long endTimestampMillis,
                      final Map<String, String> metadataQuery,
                      final Map<String, String> dimensionsQuery) throws IOException {
//        throw new UnsupportedOperationException("delete is not supported");
        return -1;
    }

    @Override
    public Map<Long, Double> aggregate(final String namespace,
                                       final String dimension,
                                       final long startTimestampMillis,
                                       final long endTimestampMillis,
                                       final Map<String, String> metadataQuery,
                                       final Map<String, String> dimensionsQuery,
                                       final int aggregateIntervalMillis,
                                       final AggregationFunction aggregationFunction) throws IOException {
//        throw new UnsupportedOperationException("aggregate is not supported");
        return Collections.emptyMap();
    }

    @Override
    public Set<String> metadata(final String namespace,
                                final String metadataKey,
                                final long startTimestampMillis,
                                final long endTimestampMillis,
                                final Map<String, String> metadataQuery,
                                final Map<String, String> dimensionsQuery) throws IOException {
        // not implemented yet
        return Collections.emptySet();
    }

    @Override
    public void expire(final String namespace, final long endTimestampMillis) throws IOException {
        checkExpire(namespace, endTimestampMillis);
        checkNamespace(namespace);
        try {
            doExpire(namespace, endTimestampMillis);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception expiring events from namespace: " + namespace, e);
            throw new IOException("exception expiring events from namespace: " + namespace, e);
        }
    }

    @Override
    protected String getObjectKeyPrefix(final String namespace) {
        return String.format(objectKeyPrefix, namespace);
    }

    private LoadingCache<String, AtomicLong> payloadOffset = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, AtomicLong>() {
                @Override
                public AtomicLong load(final String path) {
                    return new AtomicLong(0);
                }
            });

    private Logger initSiftingLogger() {
        final LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        final SiftingAppender siftingAppender = new SiftingAppender();
        final String loggerName = "cantor-s3-events-sifting-logger";
        siftingAppender.setName(loggerName);
        siftingAppender.setContext(loggerContext);

        final MDCBasedDiscriminator discriminator = new MDCBasedDiscriminator();
        discriminator.setKey("path");
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
    private synchronized void doStore(final String namespace, final Collection<Event> batch) throws IOException {
        for (final Event event : batch) {
            final Map<String, String> metadata = new HashMap<>(event.getMetadata());
            final Map<String, Double> dimensions = new HashMap<>(event.getDimensions());
            final byte[] payload = event.getPayload();

            final String currentCycleName = getRolloverCycleName();
            final String cyclePath = getPath(currentCycleName);
            final String filePath = String.format("%s/%s/%s.%s",
                    cyclePath, trim(namespace), directoryFormatter.format(event.getTimestampMillis()), currentCycleName
            );
            final String payloadFilePath = filePath + ".b64";
            final String eventsFilePath = filePath + ".json";

            if (payload != null && payload.length > 0) {
                final String payloadBase64 = Base64.getEncoder().encodeToString(payload);
                append(payloadFilePath, payloadBase64);

                // one for new line at the end of the base64 encoded byte array
                final long offset = this.payloadOffset.getUnchecked(payloadFilePath).getAndAdd(payloadBase64.length() + 1);
                dimensions.put(dimensionKeyPayloadOffset, (double) offset);
                dimensions.put(dimensionKeyPayloadLength, (double) payloadBase64.length());
            }
            final Event toWrite = new Event(event.getTimestampMillis(), metadata, dimensions);
            append(eventsFilePath, parser.toJson(toWrite));
        }
    }

    private void append(final String path, final String message) {
        MDC.put("path", path);
        this.siftingLogger.info(message);
        MDC.remove("path");
    }

    private List<Event> doGet(final String namespace,
                              final long startTimestampMillis,
                              final long endTimestampMillis,
                              final Map<String, String> metadataQuery,
                              final Map<String, String> dimensionsQuery,
                              final boolean includePayloads,
                              final boolean ascending,
                              final int limit) throws IOException {

        // find all matching log files
        final List<String> matchingKeys = getMatchingKeys(namespace, startTimestampMillis, endTimestampMillis);
        logger.info("matching files are: {}", matchingKeys);

        // using tree map for convenient sorting
        final Map<Long, Event> events = (ascending) ? new TreeMap<>() : new TreeMap<>(Collections.reverseOrder());
        final Map<String, Pattern> metadataPatterns = generateRegex(metadataQuery);
        for (final String objectKey : matchingKeys) {
            if (!objectKey.endsWith("json")) {
                continue;
            }
            final String query = generateQuery(startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
            final InputStream jsonLines = S3Utils.S3Select.queryObjectJson(this.s3Client, this.bucketName, objectKey, query);
            final Scanner lineReader = new Scanner(jsonLines);
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
                    final String payloadFilename = objectKey.replace("json", "b64");
                    final byte[] payloadBase64Bytes = S3Utils.getObjectBytes(this.s3Client, this.bucketName, payloadFilename, offset, offset + length - 1);
                    if (payloadBase64Bytes == null || payloadBase64Bytes.length == 0) {
                        throw new IOException("failed to retrieve payload for event");
                    }
                    final byte[] payload = Base64.getDecoder().decode(new String(payloadBase64Bytes));
                    final Event eventWithPayload = new Event(event.getTimestampMillis(), event.getMetadata(), event.getDimensions(), payload);
                    if (matches(eventWithPayload, metadataPatterns)) {
                        events.put(event.getTimestampMillis(), eventWithPayload);
                    }
                } else {
                    if (matches(event, metadataPatterns)) {
                        events.put(event.getTimestampMillis(), event);
                    }
                }

            }
            // files are already sorted, so we are guaranteed to have all the correct events once we hit the limit
            if (limit > 0 && events.size() >= limit) {
                break;
            }
        }

        final ArrayList<Event> orderedEvents = new ArrayList<>(events.values());
        // events may include more than the limit as files must to read in their entirety as order is not guaranteed
        return (limit <= 0) ? orderedEvents : orderedEvents.subList(0, limit);
    }

    private void doExpire(final String namespace, final long endTimestampMillis) throws IOException {
        // TODO this has to be implemented properly
        logger.info("expiring namespace '{}' with end timestamp of '{}'", namespace, endTimestampMillis);
        final List<String> keys = getMatchingKeys(namespace, 0, endTimestampMillis);
        logger.info("expiring objects: {}", keys);
        S3Utils.deleteObjects(this.s3Client, this.bucketName, keys);
    }

    // creates an s3 select compatible query
    // see https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-select.html
    private String generateQuery(final long startTimestampMillis,
                                 final long endTmestampMillis,
                                 final Map<String, String> metadataQuery,
                                 final Map<String, String> dimensionsQuery) {
        final String timestampClause = String.format("s.timestampMillis BETWEEN %d AND %d", startTimestampMillis, endTmestampMillis);
        return String.format("SELECT * FROM s3object[*] s WHERE %s %s %s",
                timestampClause,
                getMetadataQuerySql(metadataQuery, false),
                getDimensionQuerySql(dimensionsQuery, false)
        );
    }

    private List<String> getMatchingKeys(final String namespace, final long startTimestampMillis, final long endTimestampMillis)
            throws IOException {
        final Set<String> prefixes = new HashSet<>();

        long start = startTimestampMillis;
        while (start <= endTimestampMillis) {
            if (endTimestampMillis - start < TimeUnit.HOURS.toMillis(1)) {
                prefixes.add(String.format("%s/%s", trim(namespace), this.directoryFormatter.format(start)));
                start += TimeUnit.MINUTES.toMillis(1);
            } else if (endTimestampMillis - start < TimeUnit.DAYS.toMillis(1)) {
                prefixes.add(String.format("%s/%s", trim(namespace), this.directoryFormatterHourly.format(start)));
                start += TimeUnit.HOURS.toMillis(1);
            } else if (endTimestampMillis - start < TimeUnit.DAYS.toMillis(28)) {
                prefixes.add(String.format("%s/%s", trim(namespace), this.directoryFormatterDayly.format(start)));
                start += TimeUnit.DAYS.toMillis(1);
            } else if (endTimestampMillis - start < TimeUnit.DAYS.toMillis(364)) {
                prefixes.add(String.format("%s/%s", trim(namespace), this.directoryFormatterMonthly.format(start)));
                start += TimeUnit.DAYS.toMillis(28);
            } else {
                prefixes.add(String.format("%s/%s", trim(namespace), this.directoryFormatterYearly.format(start)));
                start += TimeUnit.DAYS.toMillis(364);
            }
        }
        logger.info("prefixes are: {}", prefixes);
        final List<String> matchingKeys = new ArrayList<>();
        for (final String prefix : prefixes) {
            matchingKeys.addAll(S3Utils.getKeys(this.s3Client, this.bucketName, prefix));
        }
        return matchingKeys;
    }

    // do full regex evaluation server side as s3 select only supports limited regex
    private boolean matches(final Event event, final Map<String, Pattern> metadataPatterns) {
        for (final Map.Entry<String, Pattern> metaRegex : metadataPatterns.entrySet()) {
            final String metadataValue = event.getMetadata().get(metaRegex.getKey().substring(2));
            if (metadataValue == null) {
                return false;
            }

            final Matcher regex = metaRegex.getValue().matcher(metadataValue);
            if (metaRegex.getKey().startsWith("_~") && !regex.matches()) {
                return false;
            } else if (metaRegex.getKey().startsWith("!~") && regex.matches()) {
                return false;
            }
        }
        return true;
    }

    // full regex is not supported by s3 select, so some evaluation must be done server side
    private Map<String, Pattern> generateRegex(final Map<String, String> metadataQuery) {
        final Map<String, Pattern> regexes = new HashMap<>();
        for (final Map.Entry<String, String> entry : metadataQuery.entrySet()) {
            try {
                final String maybeRegex = entry.getValue();
                // add prefix to key for easy differentiation later
                if (maybeRegex.startsWith("~")) {
                    final String fullRegex = maybeRegex.substring(1).replaceAll("(\\.?)\\*", ".*");
                    regexes.put("_~" + entry.getKey(), Pattern.compile(fullRegex));
                } else if (maybeRegex.startsWith("!~")) {
                    final String fullRegex = maybeRegex.substring(2).replaceAll("(\\.?)\\*", ".*");
                    regexes.put("!~" + entry.getKey(), Pattern.compile(fullRegex));
                }
            } catch (final PatternSyntaxException pse) {
                //TODO: we could add logic to explicitly look for limit regex, but it's simpler to just let it fall into this exception
                logger.warn("invalid regex pattern caught; will allow as limited regex may cause this exception", pse);
            }
        }
        return regexes;
    }

    // the metadata query object can contain these patterns:
    // '' (just a string): equals - 'user-id' => 'user-1'
    // '=': equals - 'user-id' => '=user-1'
    // '!=': not equals - 'user-id' => '!=user-1'
    // '~': limited regex like - 'user-id' => '~user-*'
    // '!~': inverted limited  regex like - 'user-id' => '!~user-*'
    private String getMetadataQuerySql(final Map<String, String> metadataQuery, final boolean invert) {
        if (metadataQuery.isEmpty()) {
            return "";
        }
        final StringBuilder sql = new StringBuilder();
        for (final Map.Entry<String, String> entry : metadataQuery.entrySet()) {
            final String metadataName = prefixMetadata(entry.getKey());
            final String query = entry.getValue();
            // s3 select only supports limited regex
            if (query.startsWith("~") || query.startsWith("!~")) {
                sql.append(" AND ").append(String.format(" %s LIKE %s", metadataName, quote("%")));
            } else if (query.startsWith("=")) {
                final String operation = (invert) ? " != " : " = ";
                sql.append(" AND ").append(metadataName).append(operation).append(quote(query.substring(1)));
            } else if (query.startsWith("!=")) {
                final String operation = (invert) ? " = " : " != ";
                sql.append(" AND ").append(metadataName).append(operation).append(quote(query.substring(2)));
            } else {
                final String operation = (invert) ? " != " : " = ";
                sql.append(" AND ").append(metadataName).append(operation).append(quote(query));
            }
        }
        return sql.toString();
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
    private String getDimensionQuerySql(final Map<String, String> dimensionsQuery, final boolean invert) {
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
                final String operation = (invert) ? " < " : " >= ";
                sql.append(" AND ").append(dimensionName).append(operation).append(query.substring(2));
            } else if (query.startsWith("<=")) {
                final String operation = (invert) ? " > " : " <= ";
                sql.append(" AND ").append(dimensionName).append(operation).append(query.substring(2));
            } else if (query.startsWith(">")) {
                final String operation = (invert) ? " <= " : " > ";
                sql.append(" AND ").append(dimensionName).append(operation).append(query.substring(1));
            } else if (query.startsWith("<")) {
                final String operation = (invert) ? " >= " : " < ";
                sql.append(" AND ").append(dimensionName).append(operation).append(query.substring(1));
            } else if (query.startsWith("!=")) {
                final String operation = (invert) ? " = " : " != ";
                sql.append(" AND ").append(dimensionName).append(operation).append(query.substring(2));
            } else if (query.startsWith("=")) {
                final String operation = (invert) ? " != " : " = ";
                sql.append(" AND ").append(dimensionName).append(operation).append(query.substring(1));
            } else {
                final String operation = (invert) ? " != " : " = ";
                sql.append(" AND ").append(dimensionName).append(operation).append(query);
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
    private String rollover() {
        // cycle name is: <timestamp>-<guid>
        final String rolloverCycleName = String.format("%s.%s",
                this.cycleNameFormatter.format(System.currentTimeMillis()), UUID.randomUUID().toString().replaceAll("-", ""));
        logger.info("starting new cycle: {}", rolloverCycleName);
        return this.currentFlushCycleGuid.getAndSet(rolloverCycleName);
    }

    private String getRolloverCycleName() {
        return this.currentFlushCycleGuid.get();
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
                logger.error("buffer directory '{}' does not exist or is not writable", this.bufferDirectory);
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
            return;
        } catch (Exception e) {
            logger.warn("exception during flush", e);
        }

        final long endMillis = System.currentTimeMillis();
        final long elapsedSeconds = (endMillis - startMillis) / 1_000;
        logger.info("flush cycle elapsed time: {}s", elapsedSeconds);

        // schedule the next flush cycle immediately if elapsed time is larger than flush interval so we catch up
        this.executor.schedule(this::flush, Math.max(0, this.maxFlushIntervalSeconds - elapsedSeconds), TimeUnit.SECONDS);
    }

    private void uploadDirectory(final File toUpload) throws InterruptedException {
        final MultipleFileUpload upload = this.s3TransferManager.uploadDirectory(this.bucketName, null, toUpload, true, (file, metadata) -> {
            // set object content type to plain text
            metadata.setContentType("text/plain");
        });
        // log the upload progress
        do {
            logger.info("s3 transfer progress of '{}': {}% of {}mb",
                    toUpload.getAbsolutePath(),
                    (int) upload.getProgress().getPercentTransferred(),
                    upload.getProgress().getTotalBytesToTransfer() / (1024*1024)
            );
            Thread.sleep(1_000);
        } while (!upload.isDone());
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
}
