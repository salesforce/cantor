/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.multicloudj;

import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.common.CommonPreconditions.checkNamespace;
import static com.salesforce.cantor.common.CommonPreconditions.checkString;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkDelete;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkGet;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkKeys;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkSize;
import static com.salesforce.cantor.common.ObjectsPreconditions.checkStore;

public class ObjectsOnMulticloudj extends AbstractBaseMulticloudjNamespaceable implements StreamingObjects {
    private static final Logger logger = LoggerFactory.getLogger(ObjectsOnMulticloudj.class);

    // cantor-objects/<trimmed-namespace>/<key>
    private static final String objectKeyPrefix = "cantor-objects";

    public ObjectsOnMulticloudj(final BucketClient bucketClient) throws IOException {
        super(bucketClient);
    }

    @Override
    public void store(final String namespace, final String key, final byte[] bytes) throws IOException {
        checkStore(namespace, key, bytes);
        try {
            doStore(namespace, key, new ByteArrayInputStream(bytes), bytes.length);
        } catch (final SubstrateSdkException e) {
            logger.warn("exception storing object: " + namespace + "." + key, e);
            throw new IOException("exception storing object: " + namespace + "." + key, e);
        }
    }

    @Override
    public byte[] get(final String namespace, final String key) throws IOException {
        checkGet(namespace, key);
        try {
            return doGet(namespace, key);
        } catch (final SubstrateSdkException e) {
            logger.warn("exception getting object: " + namespace + "." + key, e);
            throw new IOException("exception getting object: " + namespace + "." + key, e);
        }
    }

    @Override
    public boolean delete(final String namespace, final String key) throws IOException {
        checkDelete(namespace, key);
        try {
            return MulticloudjUtils.deleteObject(this.bucketClient, getObjectKey(namespace, key));
        } catch (final SubstrateSdkException e) {
            logger.warn("exception deleting object: " + namespace + "." + key, e);
            throw new IOException("exception deleting object: " + namespace + "." + key, e);
        }
    }

    @Override
    public Collection<String> keys(final String namespace, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count);
        try {
            return doKeys(namespace, "", start, count);
        } catch (final SubstrateSdkException e) {
            logger.warn("exception getting keys of namespace: " + namespace, e);
            throw new IOException("exception getting keys of namespace: " + namespace, e);
        }
    }

    @Override
    public Collection<String> keys(final String namespace, final String prefix, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count);
        try {
            return doKeys(namespace, prefix, start, count);
        } catch (final SubstrateSdkException e) {
            logger.warn("exception getting keys of namespace: " + namespace, e);
            throw new IOException("exception getting keys of namespace: " + namespace, e);
        }
    }

    @Override
    public int size(final String namespace) throws IOException {
        checkSize(namespace);
        try {
            return doSize(namespace);
        } catch (final SubstrateSdkException e) {
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
        } catch (final SubstrateSdkException e) {
            logger.warn("exception storing stream:", e);
            throw new IOException("exception storing stream", e);
        }
    }

    @Override
    public InputStream stream(final String namespace, final String key) throws IOException {
        checkString(namespace);
        checkString(key);
        try {
            return doStream(namespace, key);
        } catch (final SubstrateSdkException e) {
            logger.warn("exception streaming:", e);
            throw new IOException("exception streaming object: " + namespace + "." + key, e);
        }
    }

    private void doStore(final String namespace, final String key, final InputStream stream, final long length) {
        checkNamespace(namespace);
        final String objectName = getObjectKey(namespace, key);
        logger.info("storing stream with length={} at '{}.{}'", length, this.bucketClient.getBucket(), objectName);
        MulticloudjUtils.putObject(this.bucketClient, objectName, stream, length);
    }

    private byte[] doGet(final String namespace, final String key) throws IOException {
        final String objectName = getObjectKey(namespace, key);
        logger.debug("retrieving object at '{}.{}'", this.bucketClient.getBucket(), objectName);
        if (!MulticloudjUtils.doesObjectExist(this.bucketClient, objectName)) {
            return null;
        }
        return MulticloudjUtils.getObjectBytes(this.bucketClient, objectName);
    }

    private InputStream doStream(final String namespace, final String key) throws IOException {
        final String objectName = getObjectKey(namespace, key);
        if (!MulticloudjUtils.doesObjectExist(this.bucketClient, objectName)) {
            throw new IOException(String.format("couldn't find objectName '%s' for namespace '%s'", objectName, namespace));
        }
        return MulticloudjUtils.getObjectStream(this.bucketClient, objectName);
    }

    private int doSize(final String namespace) {
        // total prefix listing includes the namespace marker; subtract it from the count
        return Math.max(0, MulticloudjUtils.getSize(this.bucketClient, getObjectKey(namespace, "")) - 1);
    }

    private Collection<String> doKeys(final String namespace, final String prefix, final int start, final int count) {
        final String namespaceObjectPrefix = getObjectKey(namespace, prefix);
        // request one extra to absorb the namespace marker that's filtered out below;
        // count == -1 means unbounded, so leave it alone in that case
        final int requestCount = count > 0 ? count + 1 : count;
        final List<String> filtered = MulticloudjUtils.getKeys(this.bucketClient, namespaceObjectPrefix, start, requestCount)
                .stream()
                .filter(key -> !key.endsWith(NAMESPACE_IDENTIFIER))
                .map(objectFile -> objectFile.substring(namespaceObjectPrefix.length()))
                .collect(Collectors.toList());
        // trim to the originally requested count if we asked for one extra
        if (count > 0 && filtered.size() > count) {
            return filtered.subList(0, count);
        }
        return filtered;
    }

    private String getObjectKey(final String namespace, final String key) {
        return String.format("%s/%s", getObjectKeyPrefix(namespace), key);
    }

    @Override
    protected String getObjectKeyPrefix(final String namespace) {
        return String.format("%s/%s", objectKeyPrefix, trim(namespace));
    }
}
