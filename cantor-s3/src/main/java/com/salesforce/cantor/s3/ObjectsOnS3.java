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

import java.io.*;
import java.util.Collection;
import java.util.stream.Collectors;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.common.CommonPreconditions.checkString;
import static com.salesforce.cantor.common.ObjectsPreconditions.*;

public class ObjectsOnS3 extends AbstractBaseS3Namespaceable implements StreamingObjects {
    private static final Logger logger = LoggerFactory.getLogger(ObjectsOnS3.class);

    // cantor-object-[<namespace>]-<key>
    private static final String objectKeyFormat = "cantor-object-[%s]-%s";

    public ObjectsOnS3(final AmazonS3 s3Client, final String bucketName) throws IOException {
        super(s3Client, bucketName, "objects");
    }

    @Override
    public void store(final String namespace, final String key, final byte[] bytes) throws IOException {
        checkStore(namespace, key, bytes);
        try {
            doStore(namespace, key, new ByteArrayInputStream(bytes), bytes.length);
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
            return S3Utils.deleteObject(this.s3Client, this.bucketName, getObjectKey(namespace, key));
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

    @Override
    protected String getObjectKeyPrefix(final String namespace) {
        return getObjectKey(namespace, "");
    }

    private void doStore(final String namespace, final String key, final InputStream stream, final long length) throws IOException {
        checkNamespace(namespace);
        final String objectName = getObjectKey(namespace, key);

        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(length);
        logger.info("storing stream with length={} at '{}.{}'", length, this.bucketName, objectName);
        // if no exception is thrown, the object was put successfully - ignore response value
        S3Utils.putObject(this.s3Client, this.bucketName, objectName, stream, metadata);
    }

    private byte[] doGet(final String namespace, final String key) throws IOException {
        final String objectName = getObjectKey(namespace, key);
        logger.debug("retrieving object at '{}.{}'", this.bucketName, objectName);
        return S3Utils.getObjectBytes(this.s3Client, this.bucketName, objectName);
    }

    private InputStream doStream(final String namespace, final String key) throws IOException {
        final String objectName = getObjectKey(namespace, key);
        if (!this.s3Client.doesObjectExist(this.bucketName, objectName)) {
            throw new IOException(String.format("couldn't find objectName '%s' for namespace '%s'", objectName, namespace));
        }

        return S3Utils.getObjectStream(this.s3Client, this.bucketName, objectName);
    }

    private int doSize(final String namespace) {
        return S3Utils.getSize(this.s3Client, this.bucketName, getObjectKey(namespace, ""));
    }

    private Collection<String> doKeys(final String namespace, final int start, final int count) throws IOException {
        final String namespaceObjectPrefix = getObjectKey(namespace, "");
        return S3Utils.getKeys(this.s3Client, this.bucketName, namespaceObjectPrefix, start, count)
                .stream()
                .map(objectFile -> objectFile.substring(namespaceObjectPrefix.length()))
                .collect(Collectors.toList());
    }

    private String getObjectKey(final String namespace, final String key) {
        return String.format(objectKeyFormat, namespace, key);
    }
}
