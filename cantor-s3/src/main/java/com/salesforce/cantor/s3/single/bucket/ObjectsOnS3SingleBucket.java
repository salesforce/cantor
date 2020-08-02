/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.s3.single.bucket;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.salesforce.cantor.s3.StreamingObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.common.CommonPreconditions.checkCreate;
import static com.salesforce.cantor.common.CommonPreconditions.checkDrop;
import static com.salesforce.cantor.common.CommonPreconditions.checkString;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkDelete;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkGet;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkKeys;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkSize;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkStore;

public class ObjectsOnS3SingleBucket implements StreamingObjects {
    private static final Logger logger = LoggerFactory.getLogger(ObjectsOnS3SingleBucket.class);

    private static final String bucketNameFormat = "%s-cantor-namespace-%s";

    // read objects in 4MB chunks
    private static final int streamingChunkSize = 4 * 1024 * 1024;

    private final AmazonS3 s3Client;
    private final String bucketNameAllNamespaces;

    public ObjectsOnS3SingleBucket(final AmazonS3 s3Client) throws IOException {
        this(s3Client, "default");
    }

    public ObjectsOnS3SingleBucket(final AmazonS3 s3Client, final String bucketPrefix) throws IOException {
        // todo: ensure bucket prefix doesn't include periods (or at least doesn't start/end w/ periods)
        checkArgument(s3Client != null, "null s3 client");
        checkString(bucketPrefix, "null/empty bucket prefix");
        this.s3Client = s3Client;
        this.bucketNameAllNamespaces = String.format("%s-all-namespaces", bucketPrefix);
        try {
            if (!this.s3Client.doesBucketExistV2(bucketNameAllNamespaces)) {
                this.s3Client.createBucket(bucketNameAllNamespaces);
            }
        } catch (final AmazonS3Exception e) {
            logger.warn("exception creating required buckets for objects on s3:", e);
            throw new IOException("exception creating required buckets for objects on s3:", e);
        }
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        try {
            return doGetNamespaces();
        } catch (final AmazonS3Exception e) {
            logger.warn("exception getting namespaces:", e);
            throw new IOException("exception getting namespaces:", e);
        }
    }

    @Override
    public void create(final String namespace) throws IOException {
        checkCreate(namespace);
        try {
            doCreate(namespace);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception creating namespace:", e);
            throw new IOException("exception creating namespace: ", e);
        }
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        try {
            doDrop(namespace);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception dropping namespace:", e);
            throw new IOException("exception dropping namespace: ", e);
        }
    }

    @Override
    public void store(final String namespace, final String key, final byte[] bytes) throws IOException {
        checkStore(namespace, key, bytes);
        try {
            doStore(namespace, key, bytes);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception storing namespace:", e);
            throw new IOException("exception storing namespace: ", e);
        }
    }

    @Override
    public byte[] get(final String namespace, final String key) throws IOException {
        checkGet(namespace, key);
        try {
            return doGet(namespace, key);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception getting namespace:", e);
            throw new IOException("exception getting namespace: ", e);
        }
    }

    @Override
    public boolean delete(final String namespace, final String key) throws IOException {
        checkDelete(namespace, key);
        try {
            return doDelete(namespace, key);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception deleting namespace:", e);
            throw new IOException("exception deleting namespace: ", e);
        }
    }

    @Override
    public Collection<String> keys(final String namespace, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count);
        try {
            return doKeys(namespace, start, count);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception getting keys:", e);
            throw new IOException("exception getting keys: ", e);
        }
    }

    @Override
    public int size(final String namespace) throws IOException {
        checkSize(namespace);
        try {
            return doSize(namespace);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception getting size:", e);
            throw new IOException("exception getting size: ", e);
        }
    }

    @Override
    public void store(final String namespace, final String key, final InputStream stream, final long length) throws IOException {
        checkString(namespace);
        checkString(key);
        checkArgument(stream != null, "null stream");
        checkArgument(length > 0, "zero/negative length");
        try {
            doStore(namespace, key, stream, length);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception storing stream:", e);
        }
    }

    @Override
    public InputStream stream(final String namespace, final String key) throws IOException {
        checkString(namespace);
        checkString(key);
        try {
            return doStream(namespace, key);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception streaming:", e);
            return null;
        }
    }

    private void doCreate(final String namespace) throws IOException {
        // TODO: should we add some protection so data will not be stored in the wrong namespaces because of a typo?
        logger.debug("namespaces do not need to be created");
    }

    private void doDrop(final String namespace) throws IOException {
        if (!this.s3Client.doesBucketExistV2(this.bucketNameAllNamespaces)) {
            logger.debug("bucket '{}' does not exist; ignoring drop", this.bucketNameAllNamespaces);
            return;
        }

        logger.info("bucket '{}' exists; dropping objects with prefix '{}'", this.bucketNameAllNamespaces, namespace);
        // delete all objects
        ObjectListing objectListing = this.s3Client.listObjects(this.bucketNameAllNamespaces, namespace);
        while (true) {
            for (final S3ObjectSummary summary : objectListing.getObjectSummaries()) {
                this.s3Client.deleteObject(this.bucketNameAllNamespaces, summary.getKey());
            }
            if (objectListing.isTruncated()) {
                objectListing = this.s3Client.listNextBatchOfObjects(objectListing);
            } else {
                break;
            }
        }

        // delete all versioned objects
        VersionListing versionList = this.s3Client.listVersions(this.bucketNameAllNamespaces, namespace);
        while (true) {
            for (final S3VersionSummary summary : versionList.getVersionSummaries()) {
                this.s3Client.deleteVersion(this.bucketNameAllNamespaces, summary.getKey(), summary.getVersionId());
            }
            if (versionList.isTruncated()) {
                versionList = this.s3Client.listNextBatchOfVersions(versionList);
            } else {
                break;
            }
        }
    }

    private void doStore(final String namespace, final String key, final byte[] bytes) {
        final String objectName = getObjectName(namespace, key);
        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(bytes.length);
        logger.info("storing {} object bytes at '{}.{}'", bytes.length, this.bucketNameAllNamespaces, objectName);
        // if no exception is thrown, the object was put successfully - ignore response value
        this.s3Client.putObject(this.bucketNameAllNamespaces, objectName, new ByteArrayInputStream(bytes), metadata);
    }

    private void doStore(final String namespace, final String key, final InputStream stream, final long length) {
        final String objectName = getObjectName(namespace, key);
        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(length);
        logger.info("storing stream with length={} at '{}.{}'", length, this.bucketNameAllNamespaces, objectName);
        // if no exception is thrown, the object was put successfully - ignore response value
        this.s3Client.putObject(this.bucketNameAllNamespaces, objectName, stream, metadata);
    }

    private byte[] doGet(final String namespace, final String key) throws IOException {
        final String objectName = getObjectName(namespace, key);
        logger.debug("retrieving object at '{}.{}'", this.bucketNameAllNamespaces, objectName);
        if (!this.s3Client.doesObjectExist(this.bucketNameAllNamespaces, objectName)) {
            logger.debug("object '{}.{}' doesn't exist, returning null", this.bucketNameAllNamespaces, objectName);
            return null;
        }
        return getObjectBytes(objectName, key);
    }

    private InputStream doStream(final String namespace, final String key) throws IOException {
        final String objectName = getObjectName(namespace, key);
        if (!this.s3Client.doesObjectExist(this.bucketNameAllNamespaces, objectName)) {
            throw new IOException(String.format("couldn't find objectName '%s' for namespace '%s'", objectName, namespace));
        }

        final S3Object object = this.s3Client.getObject(this.bucketNameAllNamespaces, objectName);
        if (object == null) {
            logger.warn("object '{}.{}' should exist, but got null, returning null", this.bucketNameAllNamespaces, objectName);
            throw new IOException(String.format("couldn't find S3 object with key '%s' in bucket '%s' for namespace '%s'", objectName, this.bucketNameAllNamespaces, namespace));
        }
        return object.getObjectContent();
    }

    private byte[] getObjectBytes(final String bucket, final String key) throws IOException {
        final S3Object s3Object = this.s3Client.getObject(bucket, key);
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

    private boolean doDelete(final String namespace, final String key) throws IOException {
        final String objectName = getObjectName(namespace, key);
        if (!this.s3Client.doesObjectExist(this.bucketNameAllNamespaces, objectName)) {
            return false;
        }
        return deleteObject(objectName, key);
    }

    private int doSize(final String namespace) {
        if (!this.s3Client.doesBucketExistV2(this.bucketNameAllNamespaces)) {
            return -1;
        }

        int totalSize = 0;
        ObjectListing listing = this.s3Client.listObjects(this.bucketNameAllNamespaces, namespace);
        do {
            totalSize += listing.getObjectSummaries().size();
            logger.debug("got {} keys from {}", listing.getObjectSummaries().size(), listing);
            listing = this.s3Client.listNextBatchOfObjects(listing);
        } while (listing.isTruncated());

        return totalSize;
    }

    private Collection<String> doKeys(final String namespace, final int start, final int count) throws IOException {
        if (!this.s3Client.doesBucketExistV2(this.bucketNameAllNamespaces)) {
            throw new IOException(String.format("couldn't find bucket '%s' for namespace '%s'", this.bucketNameAllNamespaces, namespace));
        }
        return getKeys(namespace, start, count);
    }

    private Collection<String> getKeys(final String namespace, final int start, final int count) {
        final Set<String> keys = new HashSet<>();
        int index = 0;
        ObjectListing listing = this.s3Client.listObjects(this.bucketNameAllNamespaces, namespace);
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
            listing = this.s3Client.listNextBatchOfObjects(listing);
        } while (listing.isTruncated());

        return keys;
    }

    private boolean deleteObject(final String bucket, final String key) {
        this.s3Client.deleteObject(bucket, key);
        final VersionListing versionList = this.s3Client.listVersions(bucket, key);
        for (final S3VersionSummary summary : versionList.getVersionSummaries()) {
            logger.debug("deleting version {}", summary.getKey());
            this.s3Client.deleteVersion(bucket, summary.getKey(), summary.getVersionId());
        }

        return true;
    }

    private Collection<String> doGetNamespaces() {
        return getKeys(this.bucketNameAllNamespaces, 0, -1);
    }

    private String getObjectName(final String namespace, final String key) {
        return String.format(bucketNameFormat, namespace, key);
    }
}
