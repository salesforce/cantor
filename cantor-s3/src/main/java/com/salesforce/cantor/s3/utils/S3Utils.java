package com.salesforce.cantor.s3.utils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.salesforce.cantor.s3.single.bucket.ObjectsOnS3SingleBucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class S3Utils {
    private static final Logger logger = LoggerFactory.getLogger(ObjectsOnS3SingleBucket.class);

    // read objects in 4MB chunks
    private static final int streamingChunkSize = 4 * 1024 * 1024;

    public static void createBucket(final AmazonS3 s3Client, final String bucketName) throws IOException {
        if (s3Client.doesBucketExistV2(bucketName)) {
            logger.info("bucket '{}' already exists; ignoring create", bucketName);
            return;
        }
        logger.info("bucket '{}' doesn't exist; creating it", bucketName);
        s3Client.createBucket(bucketName);

        // check bucket created successfully
        if (!s3Client.doesBucketExistV2(bucketName)) {
            throw new IOException("failed to create namespace on s3 with bucket name: " + bucketName);
        }
    }

    public static void deleteBucket(final AmazonS3 s3Client, final String bucketName) throws IOException {
        deleteObjects(s3Client, bucketName, null);

        // now delete the bucket
        s3Client.deleteBucket(bucketName);

        // check bucket deleted successfully
        if (s3Client.doesBucketExistV2(bucketName)) {
            throw new IOException("failed to drop namespace on s3 with bucket name: " + bucketName);
        }
    }

    public static Collection<String> getKeys(final AmazonS3 s3Client,
                                             final String key,
                                             final String prefix,
                                             final int start,
                                             final int count) throws IOException {
        if (!s3Client.doesBucketExistV2(key)) {
            throw new IOException(String.format("couldn't find bucket '%s'", key));
        }

        final Set<String> keys = new HashSet<>();
        int index = 0;
        ObjectListing listing = s3Client.listObjects(key, prefix);
        do {
            for (final S3ObjectSummary summary : listing.getObjectSummaries()) {
                if (index < start) {
                    logger.debug("skipping {} at index={} start={}", summary.getKey(), index++, start);
                    continue;
                }
                keys.add(summary.getKey());

                if (keys.size() == count) {
                    logger.debug("retrieved {}/{} keys, returning early", keys.size(), count);
                    return keys;
                }
            }

            logger.debug("got {} keys from {}", listing.getObjectSummaries().size(), listing);
            listing = s3Client.listNextBatchOfObjects(listing);
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
        if (s3Object == null) {
            return null;
        }
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
                                              final String key) throws IOException {
        if (!s3Client.doesBucketExistV2(bucketName)) {
            throw new IOException(String.format("couldn't find bucket '%s'", bucketName));
        }

        final S3Object object = s3Client.getObject(bucketName, key);
        if (object == null) {
            logger.warn("object '{}.{}' should exist, but got null, returning null", bucketName, key);
            throw new IOException(String.format("couldn't find S3 object with key '%s' in bucket '%s'", key, bucketName));
        }

        return object.getObjectContent();
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
        final VersionListing versionList = s3Client.listVersions(bucketName, key);
        for (final S3VersionSummary summary : versionList.getVersionSummaries()) {
            logger.debug("deleting version {}", summary.getKey());
            s3Client.deleteVersion(bucketName, summary.getKey(), summary.getVersionId());
        }

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

        // delete all versioned objects
        VersionListing versionList = s3Client.listVersions(new ListVersionsRequest().withBucketName(bucketName));
        while (true) {
            for (final S3VersionSummary summary : versionList.getVersionSummaries()) {
                s3Client.deleteVersion(bucketName, summary.getKey(), summary.getVersionId());
            }
            if (versionList.isTruncated()) {
                versionList = s3Client.listNextBatchOfVersions(versionList);
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
        ObjectListing listing = s3Client.listObjects(bucket, bucketPrefix);
        do {
            totalSize += listing.getObjectSummaries().size();
            logger.debug("got {} keys from {}", listing.getObjectSummaries().size(), listing);
            listing = s3Client.listNextBatchOfObjects(listing);
        } while (listing.isTruncated());

        return totalSize;
    }
}
