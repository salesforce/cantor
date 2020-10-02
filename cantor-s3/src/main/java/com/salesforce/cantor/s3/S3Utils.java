package com.salesforce.cantor.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * This class is responsible for all direct communication to s3 objects
 */
public class S3Utils {
    private static final Logger logger = LoggerFactory.getLogger(S3Utils.class);

    // read objects in 4MB chunks
    private static final int streamingChunkSize = 4 * 1024 * 1024;

    public static Collection<String> getKeys(final AmazonS3 s3Client,
                                             final String bucketName,
                                             final String prefix) throws IOException {
        return getKeys(s3Client, bucketName, prefix, 0, -1);
    }

    public static Collection<String> getKeys(final AmazonS3 s3Client,
                                             final String bucketName,
                                             final String prefix,
                                             final int start,
                                             final int count) throws IOException {
        if (!s3Client.doesBucketExistV2(bucketName)) {
            throw new IOException(String.format("couldn't find bucket '%s'", bucketName));
        }

        final Set<String> keys = new HashSet<>();
        int index = 0;
        ObjectListing listing = null;
        do {
            if (listing == null) {
                listing = s3Client.listObjects(bucketName, prefix);
            } else {
                listing = s3Client.listNextBatchOfObjects(listing);
            }

            final List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
            // skip sections that the start index wouldn't include
            if ((objectSummaries.size() - 1) + index < start) {
                index += objectSummaries.size();
                logger.debug("skipping {} objects to index={}", objectSummaries.size(), index);
                listing = s3Client.listNextBatchOfObjects(listing);
                continue;
            }

            for (final S3ObjectSummary summary : objectSummaries) {
                if (start > index++) {
                    continue;
                }
                keys.add(summary.getKey());

                if (keys.size() == count) {
                    logger.debug("retrieved {}/{} keys, returning early", keys.size(), count);
                    return keys;
                }
            }

            logger.debug("got {} keys from {}", listing.getObjectSummaries().size(), listing);
        } while (listing.isTruncated());

        return keys;
    }

    public static byte[] getObjectBytes(final AmazonS3 s3Client,
                                        final String bucketName,
                                        final String key) throws IOException {
        if (!s3Client.doesObjectExist(bucketName, key)) {
            logger.debug("object '{}.{}' doesn't exist, returning null", bucketName, key);
            return null;
        }

        final S3Object s3Object = s3Client.getObject(bucketName, key);
        final ByteArrayOutputStream buffer;
        try (final InputStream inputStream = s3Object.getObjectContent()) {
            buffer = new ByteArrayOutputStream();
            final byte[] data = new byte[streamingChunkSize];
            int read;
            while ((read = inputStream.read(data, 0, data.length)) != -1) {
                buffer.write(data, 0, read);
            }
        }
        buffer.flush();
        return buffer.toByteArray();
    }

    public static InputStream getObjectStream(final AmazonS3 s3Client,
                                              final String bucketName,
                                              final String key) {
        if (!s3Client.doesObjectExist(bucketName, key)) {
            logger.warn(String.format("couldn't find S3 object with key '%s' in bucket '%s'", key, bucketName));
            return null;
        }

        return s3Client.getObject(bucketName, key).getObjectContent();
    }

    public static void putObject(final AmazonS3 s3Client,
                                 final String bucketName,
                                 final String key,
                                 final InputStream content,
                                 final ObjectMetadata metadata) throws IOException {
        if (!s3Client.doesBucketExistV2(bucketName)) {
            throw new IOException(String.format("couldn't find bucket '%s'", bucketName));
        }
        s3Client.putObject(bucketName, key, content, metadata);
    }

    public static boolean deleteObject(final AmazonS3 s3Client, final String bucketName, final String key) {
        if (!s3Client.doesObjectExist(bucketName, key)) {
            return false;
        }

        s3Client.deleteObject(bucketName, key);
        return true;
    }

    public static void deleteObjects(final AmazonS3 s3Client,
                                     final String bucketName,
                                     final String prefix) {
        if (!s3Client.doesBucketExistV2(bucketName)) {
            logger.debug("bucket '{}' does not exist; ignoring drop", bucketName);
            return;
        }

        logger.info("bucket '{}' exists; dropping it", bucketName);
        // delete all objects
        ObjectListing objectListing = s3Client.listObjects(bucketName, prefix);
        while (true) {
            for (final S3ObjectSummary summary : objectListing.getObjectSummaries()) {
                s3Client.deleteObject(bucketName, summary.getKey());
            }
            if (objectListing.isTruncated()) {
                objectListing = s3Client.listNextBatchOfObjects(objectListing);
            } else {
                break;
            }
        }
    }

    public static int getSize(final AmazonS3 s3Client, final String bucket, final String bucketPrefix) {
        if (!s3Client.doesBucketExistV2(bucket)) {
            return -1;
        }

        int totalSize = 0;
        ObjectListing listing = null;
        do {
            if (listing == null) {
                listing = s3Client.listObjects(bucket, bucketPrefix);
            } else {
                listing = s3Client.listNextBatchOfObjects(listing);
            }
            totalSize += listing.getObjectSummaries().size();
            logger.debug("got {} keys from {}", listing.getObjectSummaries().size(), listing);
        } while (listing.isTruncated());

        return totalSize;
    }

    public static String getObjectNameForNamespace(final AmazonS3 s3Client, final String bucketName, final String namespace) throws IOException {
        final String objectKey = getCleanKeyForNamespace(namespace);
        if (!s3Client.doesObjectExist(bucketName, objectKey)) {
            throw new IOException(String.format("namespace '%s' does not exist; formatted name: '%s'", namespace, objectKey));
        }
        return objectKey;
    }

    public static String getCleanKeyForNamespace(final String namespace) {
        final String cleanName = namespace.replaceAll("[^A-Za-z0-9_\\-]", "").toLowerCase();
        return String.format("cantor-%s-%s",
                cleanName.substring(0, Math.min(32, cleanName.length())), Math.abs(namespace.hashCode()));
    }

    /**
     * S3 Select is allows use of SQL queries on top of data stored in s3 that is in either JSON or CSV. This helper
     * class is a wrapper around the query requests. For more documentation refer to https://docs.aws.amazon.com/AmazonS3/latest/dev/selecting-content-from-objects.html
     */
    public static class S3Select {

        public static InputStream queryObjectJson(final AmazonS3 s3Client,
                                                  final String bucket,
                                                  final String key,
                                                  final String query) {
            return queryObject(s3Client, generateJsonRequest(bucket, key, query));
        }

        public static InputStream queryObjectCsv(final AmazonS3 s3Client,
                                                 final String bucket,
                                                 final String key,
                                                 final String query) {
            return queryObject(s3Client, generateCsvRequest(bucket, key, query));
        }

        public static InputStream queryObject(final AmazonS3 s3Client,
                                              final SelectObjectContentRequest request) {
            final SelectObjectContentResult result = s3Client.selectObjectContent(request);
            return result.getPayload().getRecordsInputStream(
                new SelectObjectContentEventVisitor() {
                    @Override
                    public void visit(final SelectObjectContentEvent.StatsEvent event) {
                        logger.info("s3 select query stats: bucket={} key={} query={} bytes-scanned={} bytes-processed={}",
                                request.getBucketName(),
                                request.getKey(),
                                request.getExpression(),
                                event.getDetails().getBytesProcessed(),
                                event.getDetails().getBytesScanned());
                    }

                    @Override
                    public void visit(final SelectObjectContentEvent.EndEvent event) {
                        logger.info("s3 select query completed for bucket={} key={} query={}",
                                request.getBucketName(),
                                request.getKey(),
                                request.getExpression());
                    }
                }
            );
        }

        /**
         * Request will allow a limited for of SQL describe here: https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference.html
         */
        public static SelectObjectContentRequest generateJsonRequest(final String bucket,
                                                                     final String key,
                                                                     final String query) {
            final SelectObjectContentRequest request = new SelectObjectContentRequest();
            request.setBucketName(bucket);
            request.setKey(key);
            request.setExpression(query);
            request.setExpressionType(ExpressionType.SQL);

            // queries will be made against an array of json objects
            final InputSerialization inputSerialization = new InputSerialization();
            inputSerialization.setJson(new JSONInput().withType(JSONType.LINES));
            inputSerialization.setCompressionType(CompressionType.NONE);
            request.setInputSerialization(inputSerialization);

            // response will be a json object
            final OutputSerialization outputSerialization = new OutputSerialization();
            outputSerialization.setJson(new JSONOutput());
            request.setOutputSerialization(outputSerialization);

            return request;
        }

        /**
         * Generate an S3 Select query against a csv file
         */
        public static SelectObjectContentRequest generateCsvRequest(final String bucket,
                                                                    final String key,
                                                                    final String query) {
            final SelectObjectContentRequest request = new SelectObjectContentRequest();
            request.setBucketName(bucket);
            request.setKey(key);
            request.setExpression(query);
            request.setExpressionType(ExpressionType.SQL);

            // queries will be made against an array of json objects
            final InputSerialization inputSerialization = new InputSerialization();
            inputSerialization.setCsv(new CSVInput().withFileHeaderInfo(FileHeaderInfo.USE).withFieldDelimiter(","));
            inputSerialization.setCompressionType(CompressionType.NONE);
            request.setInputSerialization(inputSerialization);

            // response will be a json object
            final OutputSerialization outputSerialization = new OutputSerialization();
            outputSerialization.setCsv(new CSVOutput());
            request.setOutputSerialization(outputSerialization);

            return request;
        }

    }
}
