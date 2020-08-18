/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.salesforce.cantor.s3.utils.S3Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.common.CommonPreconditions.checkCreate;
import static com.salesforce.cantor.common.CommonPreconditions.checkDrop;
import static com.salesforce.cantor.common.CommonPreconditions.checkString;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkDelete;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkGet;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkKeys;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkSize;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkStore;
import static com.salesforce.cantor.s3.utils.S3Utils.createBucket;
import static com.salesforce.cantor.s3.utils.S3Utils.deleteBucket;
import static com.salesforce.cantor.s3.utils.S3Utils.deleteObject;
import static com.salesforce.cantor.s3.utils.S3Utils.getKeys;
import static com.salesforce.cantor.s3.utils.S3Utils.getObjectBytes;
import static com.salesforce.cantor.s3.utils.S3Utils.getObjectStream;
import static com.salesforce.cantor.s3.utils.S3Utils.putObject;

public class ObjectsOnS3 implements StreamingObjects {
    private static final Logger logger = LoggerFactory.getLogger(ObjectsOnS3.class);

    private static final String bucketNameFormat = "%s-cantor-namespace-%d";

    private final AmazonS3 s3Client;
    private final String bucketPrefix;
    private final String bucketNameAllNamespaces;

    public ObjectsOnS3(final AmazonS3 s3Client) throws IOException {
        this(s3Client, "default");
    }

    public ObjectsOnS3(final AmazonS3 s3Client, final String bucketPrefix) throws IOException {
        // todo: ensure bucket prefix doesn't include periods (or at least doesn't start/end w/ periods)
        checkArgument(s3Client != null, "null s3 client");
        checkString(bucketPrefix, "null/empty bucket prefix");
        this.s3Client = s3Client;
        this.bucketPrefix = bucketPrefix;
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
            logger.warn("exception getting namespaces", e);
            throw new IOException("exception getting namespaces", e);
        }
    }

    @Override
    public void create(final String namespace) throws IOException {
        checkCreate(namespace);
        try {
            doCreate(namespace);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception creating namespace: " + namespace, e);
            throw new IOException("exception creating namespace: " + namespace, e);
        }
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        try {
            doDrop(namespace);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception dropping namespace: " + namespace, e);
            throw new IOException("exception dropping namespace: " + namespace, e);
        }
    }

    @Override
    public void store(final String namespace, final String key, final byte[] bytes) throws IOException {
        checkStore(namespace, key, bytes);
        try {
            doStore(namespace, key, bytes);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception storing object: " + namespace + "." + key, e);
            throw new IOException("exception storing object: " + namespace + "." + key, e);
        }
    }

    @Override
    public byte[] get(final String namespace, final String key) throws IOException {
        checkGet(namespace, key);
        try {
            return doGet(namespace, key);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception getting object: " + namespace + "." + key, e);
            throw new IOException("exception getting object: " + namespace + "." + key, e);
        }
    }

    @Override
    public boolean delete(final String namespace, final String key) throws IOException {
        checkDelete(namespace, key);
        try {
            return doDelete(namespace, key);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception deleting object: " + namespace + "." + key, e);
            throw new IOException("exception deleting object: " + namespace + "." + key, e);
        }
    }

    @Override
    public Collection<String> keys(final String namespace, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count);
        try {
            return doKeys(namespace, start, count);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception getting keys of namespace: " + namespace, e);
            throw new IOException("exception getting keys of namespace: " + namespace, e);
        }
    }

    @Override
    public int size(final String namespace) throws IOException {
        checkSize(namespace);
        try {
            return doSize(namespace);
        } catch (final AmazonS3Exception e) {
            logger.warn("exception getting size of namespace: " + namespace, e);
            throw new IOException("exception getting size of namespace: " + namespace, e);
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
        final String bucket = toBucketName(namespace);
        createBucket(this.s3Client, bucket);
        // keep a record of the namespace in the namespaces bucket
        putObject(this.s3Client, bucketNameAllNamespaces, namespace, new ByteArrayInputStream(bucket.getBytes()), new ObjectMetadata());
    }

    private void doDrop(final String namespace) throws IOException {
        final String bucket = toBucketName(namespace);
        deleteBucket(this.s3Client, bucket);
        // delete the record in the namespaces bucket
        deleteObject(this.s3Client, bucketNameAllNamespaces, namespace);
    }

    private void doStore(final String namespace, final String key, final byte[] bytes) throws IOException {
        doStore(namespace, key, new ByteArrayInputStream(bytes), bytes.length);
    }

    private void doStore(final String namespace, final String key, final InputStream stream, final long length) throws IOException {
        final String bucket = toBucketName(namespace);
        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(length);
        logger.info("storing stream with length={} at '{}.{}'", length, bucket, key);
        // if no exception is thrown, the object was put successfully - ignore response value
        putObject(this.s3Client, bucket, key, stream, metadata);
    }

    private byte[] doGet(final String namespace, final String key) throws IOException {
        final String bucket = toBucketName(namespace);
        logger.debug("retrieving object at '{}.{}'", bucket, key);
        return getObjectBytes(this.s3Client, bucket, key);
    }

    private InputStream doStream(final String namespace, final String key) throws IOException {
        final String bucket = toBucketName(namespace);
        logger.debug("retrieving object as stream at '{}.{}'", bucket, key);
        return getObjectStream(this.s3Client, bucket, key);
    }

    private boolean doDelete(final String namespace, final String key) {
        final String bucket = toBucketName(namespace);
        logger.debug("deleting object at '{}.{}'", bucket, key);
        return deleteObject(this.s3Client, bucket, key);
    }

    private int doSize(final String namespace) {
        final String bucket = toBucketName(namespace);
        logger.debug("getting size of bucket '{}'", bucket);
        return S3Utils.getSize(this.s3Client, bucket, null);
    }

    private Collection<String> doKeys(final String namespace, final int start, final int count) throws IOException {
        final String bucket = toBucketName(namespace);
        logger.debug("getting {} keys for namespace '{}' starting at {}", count, bucket, start);
        return getKeys(this.s3Client, bucket, null, start, count);
    }

    private Collection<String> doGetNamespaces() throws IOException {
        // get all objects in the bucket
        return getKeys(this.s3Client, this.bucketNameAllNamespaces, null, 0, -1);
    }

    private String toBucketName(final String namespace) {
        return String.format(bucketNameFormat, this.bucketPrefix, Math.abs(namespace.hashCode()));
    }
}
