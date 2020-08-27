package com.salesforce.cantor.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.util.IOUtils;
import com.google.gson.*;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.s3.utils.S3Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.salesforce.cantor.common.EventsPreconditions.checkGet;
import static com.salesforce.cantor.common.EventsPreconditions.checkStore;

public class EventsOnS3 extends AbstractBaseS3Namespaceable implements Events {
    private static final Logger logger = LoggerFactory.getLogger(EventsOnS3.class);
    // custom gson parser to auto-convert payload to byte[]
    private static final Gson parser = new GsonBuilder()
            .registerTypeHierarchyAdapter(byte[].class, new ByteArrayHandler()).create();

    // cantor-namespace-<namespace>
    private static final String namespaceFileFormat = "cantor-events-namespace-%s";
    // cantor-events-[<namespace>]-<startTimestamp>-<endTimestamp>
    private static final String objectKeyPrefix = "cantor-events-[%s]-";
    private static final String objectKeyFormat = objectKeyPrefix + "%d-%d";
    private static final Pattern eventsObjectPattern = Pattern.compile("cantor-events-\\[(?<namespace>.*)]-(?<start>\\d+)-(?<end>\\d+)");
    private static final long chunkMillis = TimeUnit.HOURS.toMillis(1);

    public EventsOnS3(final AmazonS3 s3Client, final String bucketName) throws IOException {
        super(s3Client, bucketName);
    }

    @Override
    public void store(final String namespace, final Collection<Event> batch) throws IOException {
        checkStore(namespace, batch);
        try {
            doStore(namespace, batch);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception storing events to namespace: " + namespace, e);
            throw new IOException("exception storing {} events to namespace: " + namespace, e);
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
        try {
            return doGet(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery, includePayloads, ascending, limit);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception getting events from namespace: " + namespace, e);
            throw new IOException("exception storing {} events to namespace: " + namespace, e);
        }
    }

    @Override
    public int delete(final String namespace,
                      final long startTimestampMillis,
                      final long endTimestampMillis,
                      final Map<String, String> metadataQuery,
                      final Map<String, String> dimensionsQuery) throws IOException {
        // not implemented yet
        return 0;
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
        // not implemented yet
    }

    @Override
    protected String getNamespaceKey(final String namespace) {
        return String.format(namespaceFileFormat, namespace);
    }

    @Override
    protected String getObjectKeyPrefix(final String namespace) {
        return String.format(objectKeyPrefix, namespace);
    }

    // storing each event in json lines format to conform to s3 selects preferred format; see https://docs.aws.amazon.com/AmazonS3/latest/dev/selecting-content-from-objects.html
    private void doStore(final String namespace, final Collection<Event> batch) throws IOException {
        if (!super.namespaceExists(namespace)) {
            throw new IOException(String.format("namespace '%s' doesn't exist; can't store object with key '%s'", namespace, getNamespaceKey(namespace)));
        }

        final Map<String, OutputStream> keyToObject = new HashMap<>();
        final Collection<String> eventsFiles = S3Utils.getKeys(this.s3Client, this.bucketName, getObjectKeyPrefix(namespace));
        for (final Event event : batch) {
            final String key = getObjectKey(eventsFiles, namespace, event.getTimestampMillis());
            try (final OutputStream objectStream = getOutputStream(this.bucketName, key, keyToObject)) {
                objectStream.write((parser.toJson(event) + "\n").getBytes());
            }
        }
    }

    private List<Event> doGet(final String namespace,
                              final long startTimestampMillis,
                              final long endTimestampMillis,
                              final Map<String, String> metadataQuery,
                              final Map<String, String> dimensionsQuery,
                              final boolean includePayloads,
                              final boolean ascending,
                              final int limit) throws IOException {
        // TODO: not implemented yet
        if (!super.namespaceExists(namespace)) {
            throw new IOException(String.format("namespace '%s' doesn't exist; can't retrieve object with key '%s'", namespace, getNamespaceKey(namespace)));
        }

        return null;
    }

    // simple caching logic to prevent numerous copies of the object that events will be added to
    private OutputStream getOutputStream(final String bucketName,
                                         final String key,
                                         final Map<String, OutputStream> localCache) throws IOException {
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

    // attempts to get key which container the timestamp within its bounds
    private String getObjectKey(final Collection<String> keys,
                                final String namespace,
                                final long eventTimestampMillis) throws IOException {
        final Optional<String> existingKey = keys.stream().filter(key -> {
            final Matcher matcher = eventsObjectPattern.matcher(key);
            if (matcher.matches()) {
                final long start = Long.parseLong(matcher.group("start"));
                final long end = Long.parseLong(matcher.group("end"));
                return start <= eventTimestampMillis && eventTimestampMillis <= end;
            }
            return false;
        }).findFirst();

        // else gets default form of the key that would contain the timestamp
        return existingKey.orElse(
                String.format(objectKeyFormat,
                        namespace,
                        getFloorForChunk(eventTimestampMillis),
                        getCeilingForChunk(eventTimestampMillis)
                )
        );
    }

    // returns all keys that overlap with the timeframe
    public <R> List<R> getMatchingKeys(final Collection<R> keys,
                                       final long startTimestampMillis,
                                       final long endTimestampMillis) {
        final long windowStart = getFloorForChunk(startTimestampMillis);
        final long windowEnd = (endTimestampMillis <= Long.MAX_VALUE - chunkMillis)
                ? getCeilingForChunk(endTimestampMillis)
                : endTimestampMillis;
        return keys.stream().filter(key -> {
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
                }).collect(Collectors.toList());
    }

    protected long getFloorForChunk(final long timestampMillis) {
        return (timestampMillis / chunkMillis) * chunkMillis;
    }

    protected long getCeilingForChunk(final long timestampMillis) {
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
        public byte[] deserialize(final JsonElement jsonElement, final Type type, final JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
            return jsonElement.getAsString().getBytes();
        }

        @Override
        public JsonElement serialize(final byte[] bytes, final Type type, final JsonSerializationContext jsonSerializationContext) {
            final String encodedString = Base64.getEncoder().encodeToString(bytes);
            return new JsonPrimitive(encodedString);
        }
    }
}
