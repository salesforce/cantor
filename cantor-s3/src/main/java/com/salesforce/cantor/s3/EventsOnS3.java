package com.salesforce.cantor.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.s3.utils.S3Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EventsOnS3 extends AbstractBaseS3Namespaceable implements Events {
    private static final Logger logger = LoggerFactory.getLogger(EventsOnS3.class);

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

    }

    @Override
    public List<Event> get(final String namespace, final long startTimestampMillis, final long endTimestampMillis, final Map<String, String> metadataQuery, final Map<String, String> dimensionsQuery, final boolean includePayloads, final boolean ascending, final int limit) throws IOException {
        return null;
    }

    @Override
    public int delete(final String namespace, final long startTimestampMillis, final long endTimestampMillis, final Map<String, String> metadataQuery, final Map<String, String> dimensionsQuery) throws IOException {
        return 0;
    }

    @Override
    public Map<Long, Double> aggregate(final String namespace, final String dimension, final long startTimestampMillis, final long endTimestampMillis, final Map<String, String> metadataQuery, final Map<String, String> dimensionsQuery, final int aggregateIntervalMillis, final AggregationFunction aggregationFunction) throws IOException {
        return null;
    }

    @Override
    public Set<String> metadata(final String namespace, final String metadataKey, final long startTimestampMillis, final long endTimestampMillis, final Map<String, String> metadataQuery, final Map<String, String> dimensionsQuery) throws IOException {
        return null;
    }

    @Override
    public void expire(final String namespace, final long endTimestampMillis) throws IOException {

    }

    @Override
    protected String getNamespaceKey(final String namespace) {
        return String.format(namespaceFileFormat, namespace);
    }

    @Override
    protected String getObjectKeyPrefix(final String namespace) {
        return String.format(objectKeyPrefix, namespace);
    }

    private String getObjectKey(final String namespace, final long eventTimestampMillis) throws IOException {
        final Collection<String> eventsFiles = S3Utils.getKeys(this.s3Client, this.bucketName, getObjectKeyPrefix(namespace));
        final Optional<String> targetObject = eventsFiles.stream().filter(fileName -> {
            final Matcher matcher = eventsObjectPattern.matcher(fileName);
            if (matcher.matches()) {
                final long start = Long.parseLong(matcher.group("start"));
                final long end = Long.parseLong(matcher.group("end"));
                return start <= eventTimestampMillis && eventTimestampMillis <= end;
            }
            return false;
        }).findFirst();

        return targetObject.orElseGet(() ->
            // default form of the key before compaction
            String.format(objectKeyFormat,
                namespace,
                getFloorForChunk(eventTimestampMillis),
                getCeilingForChunk(eventTimestampMillis))
        );

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
}
