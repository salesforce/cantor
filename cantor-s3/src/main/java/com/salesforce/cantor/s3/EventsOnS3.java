package com.salesforce.cantor.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.util.IOUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.*;
import com.salesforce.cantor.Events;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.salesforce.cantor.common.EventsPreconditions.*;

public class EventsOnS3 extends AbstractBaseS3Namespaceable implements Events {
    private static final Logger logger = LoggerFactory.getLogger(EventsOnS3.class);

    private static final String dimensionKeyPayloadOffset = ".payload-offset";
    private static final String dimensionKeyPayloadLength = ".payload-length";

    private final Map<String, ByteArrayOutputStream> keyToObject = new ConcurrentHashMap<>();
    private static final AtomicReference<String> currentFlushCycleGuid = new AtomicReference<>(UUID.randomUUID().toString());

    // custom gson parser to auto-convert payload to byte[]
    private static final Gson parser = new GsonBuilder()
            .registerTypeHierarchyAdapter(byte[].class, new ByteArrayHandler()).create();

    private static final Gson parserPayloadless = new GsonBuilder()
            // ignore parameters; empty payload field
            .registerTypeHierarchyAdapter(byte[].class, (JsonDeserializer<Object>) (i, i1, i2) -> new byte[0])
            .create();

    private Logger siftingLogger = LoggerFactory.getLogger("cantor-s3-sifting");
    private final ChunkFileManager manager;

    // cantor-events-<namespace>/<startTimestamp>-<endTimestamp>
    private static final String objectKeyPrefix = "cantor-events-%s/";
    private static final Pattern eventsObjectPattern = Pattern.compile("cantor-events-(?<namespace>.*)/(?<start>\\d+)-(?<end>\\d+)");
    private static final long chunkMillis = TimeUnit.HOURS.toMillis(1);

    // date formatter for converting an event timestamp to a hierarchical directory structure
    private static final DateFormat formatter = new SimpleDateFormat("YYYY/MM/dd/HH/mm");

    public EventsOnS3(final AmazonS3 s3Client,
                      final String bucketName) throws IOException {
        super(s3Client, bucketName, "events");

        this.manager = new ChunkFileManager("/tmp/cantor-s3/", s3Client, bucketName);
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
        checkDelete(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        checkNamespace(namespace);
        try {
            return doDelete(namespace,
                    startTimestampMillis,
                    endTimestampMillis,
                    (metadataQuery != null) ? metadataQuery : Collections.emptyMap(),
                    (dimensionsQuery != null) ? dimensionsQuery : Collections.emptyMap());
        } catch (final AmazonS3Exception e) {
            logger.warn("exception deleting events from namespace: " + namespace, e);
            throw new IOException("exception deleting events from namespace: " + namespace, e);
        }
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
        // not implemented yet
        return null;
    }

    @Override
    public Set<String> metadata(final String namespace,
                                final String metadataKey,
                                final long startTimestampMillis,
                                final long endTimestampMillis,
                                final Map<String, String> metadataQuery,
                                final Map<String, String> dimensionsQuery) throws IOException {
        // not implemented yet
        return null;
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

    // storing each event in json lines format to conform to s3 selects preferred format
    // see https://docs.aws.amazon.com/AmazonS3/latest/dev/selecting-content-from-objects.html
    private synchronized void doStore(final String namespace, final Collection<Event> batch) throws IOException {
        for (final Event event : batch) {
            final Map<String, String> metadata = new HashMap<>(event.getMetadata());
            final Map<String, Double> dimensions = new HashMap<>(event.getDimensions());
            final byte[] payload = event.getPayload();

            final String currentCycleGuid = currentFlushCycleGuid.get();
            final String payloadFilePath = String.format("%s/%s/%s.payloads-%s.b64",
                    currentCycleGuid, trim(namespace), formatter.format(event.getTimestampMillis()), currentCycleGuid
            );
            final String eventsFilePath = String.format("%s/%s/%s.events-%s.json",
                    currentCycleGuid, trim(namespace), formatter.format(event.getTimestampMillis()), currentCycleGuid
            );

            if (payload != null && payload.length > 0) {
                final String payloadBase64 = Base64.getEncoder().encodeToString(payload);
                logMessage(payloadFilePath, payloadBase64);

                // one for new line at the end of the base64 encoded byte array
                final long offset = this.payloadOffset.getUnchecked(payloadFilePath).getAndAdd(payloadBase64.length() + 1);
                dimensions.put(dimensionKeyPayloadOffset, (double) offset);
                dimensions.put(dimensionKeyPayloadLength, (double) payloadBase64.length());
            }
            final Event toWrite = new Event(event.getTimestampMillis(), metadata, dimensions);
            logMessage(eventsFilePath, parser.toJson(toWrite));
        }
    }

    private void logMessage(final String path, final String message) {
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
        final List<String> matchingKeys = new ArrayList<>();
        for (long timestamp = startTimestampMillis; timestamp <= endTimestampMillis; timestamp += TimeUnit.HOURS.toMillis(1)) {
            final String prefix = String.format("%s/%s.events-", trim(namespace), formatter.format(timestamp));
            matchingKeys.addAll(S3Utils.getKeys(this.s3Client, this.bucketName, prefix));
        }

        logger.info("matching files are: {}", matchingKeys);

        // using tree map for convenient sorting
        final Map<Long, Event> events = (ascending) ? new TreeMap<>() : new TreeMap<>(Collections.reverseOrder());
        final Map<String, Pattern> metadataPatterns = generateRegex(metadataQuery);
        for (final String objectKey : matchingKeys) {
            if (!objectKey.endsWith("json")) {
                continue;
            }
            final String query = generateQuery(startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
            logger.info("object key: {} - query: {}", objectKey, query);
            final InputStream jsonLines = S3Utils.S3Select.queryObjectJson(this.s3Client, this.bucketName, objectKey, query);
            final Scanner lineReader = new Scanner(jsonLines);
            // json events are stored in json lines format, so one json object per line
            while (lineReader.hasNext()) {
                final Event event;
                if (!includePayloads) {
                    event = parserPayloadless.fromJson(lineReader.nextLine(), Event.class);
                } else {
                    event = parser.fromJson(lineReader.nextLine(), Event.class);
                }

                if (validEvent(event, metadataPatterns)) {
                    events.put(event.getTimestampMillis(), event);
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

    // delete can also be seen as only keeping the events that don't match the query
    private int doDelete(final String namespace,
                         final long startTimestampMillis,
                         final long endTimestampMillis,
                         final Map<String, String> metadataQuery,
                         final Map<String, String> dimensionsQuery) throws IOException {
        final Collection<String> eventsObjectKeys = S3Utils.getKeys(this.s3Client, this.bucketName, getObjectKeyPrefix(namespace));
        final List<String> matchingKeys = getMatchingKeys(eventsObjectKeys, startTimestampMillis, endTimestampMillis, true);

        final Map<String, Pattern> metadataPatterns = generateRegex(metadataQuery);
        int deleteCount = 0;
        for (final String objectKey : matchingKeys) {
            keyToObject.put(objectKey, new ByteArrayOutputStream());

            final String query = generateQueryNegative(metadataQuery, dimensionsQuery);
            final InputStream jsonLines = S3Utils.S3Select.queryObjectJson(this.s3Client, this.bucketName, objectKey, query);
            final Scanner lineReader = new Scanner(jsonLines);
            // json events are stored in json lines format, so one json object per line
            while (lineReader.hasNextLine()) {
                final Event event = parser.fromJson(lineReader.nextLine(), Event.class);
                if (event.getTimestampMillis() < startTimestampMillis || // event must be within timerange
                    event.getTimestampMillis() > endTimestampMillis || // else should not be deleted
                    !validEvent(event, metadataPatterns)) { // event matching query should be deleted
                    keyToObject.get(objectKey).write((parser.toJson(event) + "\n").getBytes(StandardCharsets.UTF_8));
                } else {
                    deleteCount++;
                }
            }
        }

        // TODO: this is an inaccurate count; events simply not returned due to the s3 select query won't be counted
        return deleteCount;
    }

    private void doExpire(final String namespace, final long endTimestampMillis) throws IOException {
        final Collection<String> eventsObjectKeys = S3Utils.getKeys(this.s3Client, this.bucketName, getObjectKeyPrefix(namespace));
        final List<String> matchingKeys = getMatchingKeys(eventsObjectKeys, 0, endTimestampMillis, true);
        final Iterator<String> objectToExpire = matchingKeys.iterator();
        while (objectToExpire.hasNext()) {
            final String objectKey = objectToExpire.next();
            if (objectToExpire.hasNext()) {
                S3Utils.deleteObject(this.s3Client, this.bucketName, objectKey);
            } else { // delete the matching events from the last object instead
                doDelete(namespace, 0, endTimestampMillis, Collections.emptyMap(), Collections.emptyMap());
            }
        }
    }

    // simple caching logic to prevent numerous copies of the object that events will be added to
    private ByteArrayOutputStream getOutputStream(final String bucketName,
                                                  final String key,
                                                  final Map<String, ByteArrayOutputStream> localCache) throws IOException {
        if (localCache.containsKey(key)) {
            return localCache.get(key);
        }
        localCache.put(key, new ByteArrayOutputStream());
        try (final InputStream objectStream = S3Utils.getObjectStream(this.s3Client, bucketName, key)) {
            if (objectStream != null) {
                IOUtils.copy(objectStream, localCache.get(key));
            }
            return localCache.get(key);
        }
    }

    // returns all keys that overlap with the timeframe
    public <R> List<R> getMatchingKeys(final Collection<R> keys,
                                       final long startTimestampMillis,
                                       final long endTimestampMillis,
                                       final boolean ascending) {
        final long windowStart = getFloorForChunk(startTimestampMillis);
        final long windowEnd = (endTimestampMillis <= Long.MAX_VALUE - chunkMillis)
                ? getCeilingForChunk(endTimestampMillis)
                : endTimestampMillis;
        final Stream<R> keysStream = keys.stream().filter(key -> {
            final Matcher matcher = eventsObjectPattern.matcher(key.toString());
            if (matcher.matches()) {
                final long fileStart = Long.parseLong(matcher.group("start"));
                final long fileEnd = Long.parseLong(matcher.group("end"));
                // -------s-------------e---------  <- start and end parameters
                // ssssssssssssssssssssss           <- first check
                //        eeeeeeeeeeeeeeeeeeeeeeee  <- second check
                // any combination of s and e the file overlaps the timeframe
                return fileStart <= windowEnd && fileEnd >= windowStart;
            }
            return false;
        });

        final List<R> matchingKeys;
        if (ascending) {
            matchingKeys = keysStream.sorted(this::ascendingSort).collect(Collectors.toList());
        } else {
            matchingKeys = keysStream.sorted(this::descendingSort).collect(Collectors.toList());
        }
        return matchingKeys;
    }

    private <R> int ascendingSort(final R keyA, final R keyB) {
        final Matcher matcherA = eventsObjectPattern.matcher(keyA.toString());
        final Matcher matcherB = eventsObjectPattern.matcher(keyB.toString());
        if (matcherA.matches() && matcherB.matches()) {
            final long fileStartA = Long.parseLong(matcherA.group("start"));
            final long fileStartB = Long.parseLong(matcherB.group("start"));
            return Long.compare(fileStartA, fileStartB);
        }
        return 0;
    }

    private <R> int descendingSort(final R keyA, final R keyB) {
        final Matcher matcherA = eventsObjectPattern.matcher(keyA.toString());
        final Matcher matcherB = eventsObjectPattern.matcher(keyB.toString());
        if (matcherA.matches() && matcherB.matches()) {
            final long fileStartA = Long.parseLong(matcherA.group("start"));
            final long fileStartB = Long.parseLong(matcherB.group("start"));
            return Long.compare(fileStartB, fileStartA);
        }
        return 0;
    }

    // creates an s3 select compatible query; see https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-select.html
    private String generateQuery(final long startTimestampMillis,
                                 final long endTimestampMillis,
                                 final Map<String, String> metadataQuery,
                                 final Map<String, String> dimensionsQuery) {
        final String timestampClause = String.format("s.timestampMillis BETWEEN %d AND %d", startTimestampMillis, endTimestampMillis);
        return String.format("SELECT * FROM s3object[*] s WHERE %s %s %s",
                timestampClause,
                getMetadataQuerySql(metadataQuery, false),
                getDimensionQuerySql(dimensionsQuery, false)
        );
    }

    // creates an s3 select query that is the negation of the two queries provided
    private String generateQueryNegative(final Map<String, String> metadataQuery,
                                         final Map<String, String> dimensionsQuery) {
        if (metadataQuery.isEmpty() && dimensionsQuery.isEmpty()) {
            return "SELECT * FROM s3object[*] s";
        }

        return String.format("SELECT * FROM s3object[*] s WHERE %s %s",
                getMetadataQuerySql(metadataQuery, true),
                getDimensionQuerySql(dimensionsQuery, true))
                .replaceFirst("AND", "");
    }

    // do full regex evaluation server side as s3 select only supports limited regex
    private boolean validEvent(final Event event, final Map<String, Pattern> metadataPatterns) {
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

    private long getFloorForChunk(final long timestampMillis) {
        return (timestampMillis / chunkMillis) * chunkMillis;
    }

    private long getCeilingForChunk(final long timestampMillis) {
        if (timestampMillis >= Long.MAX_VALUE - chunkMillis) {
            return Long.MAX_VALUE;
        }
        return getFloorForChunk(timestampMillis) + chunkMillis - 1;
    }

    /**
     * Serialize override to allow conversion of payload string to byte array
     */
    private static class ByteArrayHandler implements JsonSerializer<byte[]>, JsonDeserializer<byte[]> {

        @Override
        public byte[] deserialize(final JsonElement jsonElement,
                                  final Type type,
                                  final JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
            return Base64.getDecoder().decode(jsonElement.getAsString());
        }

        @Override
        public JsonElement serialize(final byte[] bytes,
                                     final Type type,
                                     final JsonSerializationContext jsonSerializationContext) {
            final String encodedString = Base64.getEncoder().encodeToString(bytes);
            return new JsonPrimitive(encodedString);
        }
    }

    private static class ChunkFileManager {
        private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        private final AtomicReference<Path> basePath = new AtomicReference<>();
        private final AmazonS3 s3Client;
        private final String bucketName;
        private final String baseDirectory;

        ChunkFileManager(final String baseDirectory, final AmazonS3 s3Client, final String bucketName) throws IOException {
            this.s3Client = s3Client;
            this.bucketName = bucketName;
            this.baseDirectory = baseDirectory;
            this.basePath.set(Files.createTempDirectory("cantor-s3-buffer"));
            this.executor.scheduleWithFixedDelay(this::flushToS3, 30, 30, TimeUnit.SECONDS);
        }

        private void flushToS3() {
            try {
                // rotate flush cycle guid
                final String previousFlushCycleGuid = currentFlushCycleGuid.getAndSet(UUID.randomUUID().toString());
                final List<File> toDelete = new ArrayList<>();

                final Path previousFlushCyclePath = Paths.get(this.baseDirectory + File.separator + previousFlushCycleGuid);
                logger.info("uploading buffer directory: {}", previousFlushCyclePath.toAbsolutePath().toString());
                // skip if path does not exist or is not a directory
                if (!previousFlushCyclePath.toFile().exists() || !previousFlushCyclePath.toFile().isDirectory()) {
                    logger.info("nothing to upload");
                    return;
                }

                final TransferManagerBuilder builder = TransferManagerBuilder.standard();
                builder.setS3Client(this.s3Client);
                final TransferManager manager = builder.build();


                final MultipleFileUpload upload = manager.uploadDirectory(this.bucketName, "", previousFlushCyclePath.toFile(), true, (file, objectMetadata) -> {
                    // set object content type to plain text
                    objectMetadata.setContentType("text/plain");
                    // add file to be deleted after uploading
                    toDelete.add(file);
                });
                upload.waitForCompletion();
                logger.info("successfully uploaded, removing files under '{}'", previousFlushCyclePath.toAbsolutePath().toString());
                for (final File file : toDelete) {
                    file.delete();
                }
                // TODO delete the directory
                Files.walk(previousFlushCyclePath)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            } catch (Exception e) {
                logger.warn("exception caught on uploading buffer directory", e);
            }
        }
    }
}
