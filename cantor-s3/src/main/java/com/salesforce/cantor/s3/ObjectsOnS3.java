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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.stream.Collectors;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.common.CommonPreconditions.checkCreate;
import static com.salesforce.cantor.common.CommonPreconditions.checkDrop;
import static com.salesforce.cantor.common.CommonPreconditions.checkString;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkDelete;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkGet;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkKeys;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkSize;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkStore;
import static com.salesforce.cantor.s3.utils.S3Utils.deleteObject;
import static com.salesforce.cantor.s3.utils.S3Utils.deleteObjects;
import static com.salesforce.cantor.s3.utils.S3Utils.getKeys;
import static com.salesforce.cantor.s3.utils.S3Utils.getObjectBytes;
import static com.salesforce.cantor.s3.utils.S3Utils.getObjectStream;
import static com.salesforce.cantor.s3.utils.S3Utils.getSize;
import static com.salesforce.cantor.s3.utils.S3Utils.putObject;

public class ObjectsOnS3 implements StreamingObjects {
    private static final Logger logger = LoggerFactory.getLogger(ObjectsOnS3.class);

    // cantor-namespace-<namespace>
    private static final String namespaceFileFormat = "cantor-namespace-%s";
    // cantor-object-[<namespace>]-<key>
    private static final String objectFileFormat = "cantor-object-[%s]-%s";

    private final AmazonS3 s3Client;
    private final String bucketName;

    public ObjectsOnS3(final AmazonS3 s3Client, final String bucketName) throws IOException {
        checkArgument(s3Client != null, "null s3 client");
        checkString(bucketName, "null/empty bucket name");
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        try {
            if (!this.s3Client.doesBucketExistV2(this.bucketName)) {
                this.s3Client.createBucket(this.bucketName);
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
            logger.warn("exception getting object: "  + namespace + "." + key, e);
            throw new IOException("exception getting object: " + namespace + "." + key, e);
        }
    }

    @Override
    public boolean delete(final String namespace, final String key) throws IOException {
        checkDelete(namespace, key);
        try {
            return deleteObject(this.s3Client, this.bucketName, getObjectName(namespace, key));
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
        final String key = getNamespaceKey(namespace);
        if (this.s3Client.doesObjectExist(this.bucketName, key)) {
            logger.debug("namespace '{}' already exists; no need to recreate", namespace);
        }

        logger.info("creating namespace '{}' at '{}.{}'", namespace, this.bucketName, key);
        // if no exception is thrown, the object was put successfully - ignore response value
        // TODO: in the future we can use this file to keep track of useful namespace level information like encryption type
        putObject(this.s3Client, this.bucketName, key, new ByteArrayInputStream(new byte[0]), new ObjectMetadata());
    }

    private void doDrop(final String namespace) {
        final String namespaceObjectPrefix = getObjectName(namespace, "");
        logger.info("dropping namespace '{}'", namespace);
        logger.debug("deleting all objects with prefix '{}.{}'", this.bucketName, namespaceObjectPrefix);
        deleteObjects(this.s3Client, this.bucketName, namespaceObjectPrefix);

        final String namespaceKey = getNamespaceKey(namespace);
        logger.debug("deleting namespace record object '{}.{}'", this.bucketName, namespaceKey);
        this.s3Client.deleteObject(this.bucketName, namespaceKey);
    }

    private void doStore(final String namespace, final String key, final byte[] bytes) throws IOException {
        doStore(namespace, key, new ByteArrayInputStream(bytes), bytes.length);
    }

    private void doStore(final String namespace, final String key, final InputStream stream, final long length) throws IOException {
        final String objectName = getObjectName(namespace, key);
        if (!this.s3Client.doesObjectExist(this.bucketName, getNamespaceKey(namespace))) {
            throw new IOException(String.format("namespace '%s' doesn't exist; can't store object with key '%s'", namespace, key));
        }

        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(length);
        logger.info("storing stream with length={} at '{}.{}'", length, this.bucketName, objectName);
        // if no exception is thrown, the object was put successfully - ignore response value
        putObject(this.s3Client, this.bucketName, objectName, stream, metadata);
    }

    private byte[] doGet(final String namespace, final String key) throws IOException {
        final String objectName = getObjectName(namespace, key);
        logger.debug("retrieving object at '{}.{}'", this.bucketName, objectName);
        return getObjectBytes(this.s3Client, this.bucketName, objectName);
    }

    private InputStream doStream(final String namespace, final String key) throws IOException {
        final String objectName = getObjectName(namespace, key);
        if (!this.s3Client.doesObjectExist(this.bucketName, objectName)) {
            throw new IOException(String.format("couldn't find objectName '%s' for namespace '%s'", objectName, namespace));
        }
        return getObjectStream(this.s3Client, this.bucketName, objectName);
    }

    private int doSize(final String namespace) {
        return getSize(this.s3Client, this.bucketName, getObjectName(namespace, ""));
    }

    private Collection<String> doKeys(final String namespace, final int start, final int count) throws IOException {
        final String namespaceObjectPrefix = getObjectName(namespace, "");
        return getKeys(this.s3Client, this.bucketName, namespaceObjectPrefix, start, count)
                .stream()
                .map(objectFile -> objectFile.substring(namespaceObjectPrefix.length()))
                .collect(Collectors.toList());
    }

    private Collection<String> doGetNamespaces() throws IOException {
        final String namespacePrefix = getNamespaceKey("");
        return getKeys(this.s3Client, this.bucketName, namespacePrefix, 0, -1)
                .stream()
                .map(namespaceFile -> namespaceFile.substring(namespacePrefix.length()))
                .collect(Collectors.toList());
    }

    private String getNamespaceKey(final String namespace) {
        return String.format(namespaceFileFormat, namespace);
    }

    private String getObjectName(final String namespace, final String key) {
        return String.format(objectFileFormat, namespace, key);
    }
}
