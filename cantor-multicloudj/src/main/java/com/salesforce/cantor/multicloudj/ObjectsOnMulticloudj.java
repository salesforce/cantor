/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.multicloudj;

import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.salesforce.cantor.common.ObjectsPreconditions.*;

public final class ObjectsOnMulticloudj extends AbstractBaseMulticloudjNamespaceable implements StreamingObjects {
    private static final String OBJECT_KEY_PREFIX = "cantor-objects";

    public ObjectsOnMulticloudj(final BucketClient bucketClient) throws IOException {
        super(bucketClient, "objects");
    }

    @Override
    public void store(final String namespace, final String key, final byte[] bytes) throws IOException {
        checkStore(namespace, key, bytes);
        checkNamespaceExists(namespace);
        try {
            MulticloudjUtils.upload(bucketClient, getObjectKey(namespace, key), bytes);
        } catch (final SubstrateSdkException e) {
            throw MulticloudjUtils.wrapAsIOException("store", namespace, e);
        }
    }

    @Override
    public void store(final String namespace, final String key, final InputStream stream, final long length) throws IOException {
        checkStore(namespace, key, new byte[0]);
        checkNamespaceExists(namespace);
        try {
            MulticloudjUtils.upload(bucketClient, getObjectKey(namespace, key), stream);
        } catch (final SubstrateSdkException e) {
            throw MulticloudjUtils.wrapAsIOException("store(stream)", namespace, e);
        }
    }

    @Override
    public byte[] get(final String namespace, final String key) throws IOException {
        checkGet(namespace, key);
        try {
            if (!MulticloudjUtils.doesObjectExist(bucketClient, getObjectKey(namespace, key))) {
                return null;
            }
            return MulticloudjUtils.download(bucketClient, getObjectKey(namespace, key));
        } catch (final SubstrateSdkException e) {
            throw MulticloudjUtils.wrapAsIOException("get", namespace, e);
        }
    }

    @Override
    public InputStream stream(final String namespace, final String key) throws IOException {
        checkGet(namespace, key);
        try {
            if (!MulticloudjUtils.doesObjectExist(bucketClient, getObjectKey(namespace, key))) {
                return null;
            }
            byte[] data = MulticloudjUtils.download(bucketClient, getObjectKey(namespace, key));
            return new ByteArrayInputStream(data);
        } catch (final SubstrateSdkException e) {
            throw MulticloudjUtils.wrapAsIOException("stream", namespace, e);
        }
    }

    @Override
    public boolean delete(final String namespace, final String key) throws IOException {
        checkDelete(namespace, key);
        try {
            boolean existed = MulticloudjUtils.doesObjectExist(bucketClient, getObjectKey(namespace, key));
            if (existed) {
                bucketClient.delete(getObjectKey(namespace, key), null);
            }
            return existed;
        } catch (final SubstrateSdkException e) {
            throw MulticloudjUtils.wrapAsIOException("delete", namespace, e);
        }
    }

    @Override
    public Collection<String> keys(final String namespace, final int start, final int count) throws IOException {
        return keys(namespace, "", start, count);
    }

    @Override
    public Collection<String> keys(final String namespace, final String prefix, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count);
        try {
            final String searchPrefix = getObjectKeyPrefix(namespace) + "/" + prefix;
            final String stripPrefix = getObjectKeyPrefix(namespace) + "/";
            List<String> allKeys = MulticloudjUtils.listKeysOrdered(bucketClient, searchPrefix, 0, -1);
            List<String> filtered = allKeys.stream()
                    .map(k -> k.substring(stripPrefix.length()))
                    .filter(k -> !k.equals(NAMESPACE_IDENTIFIER))
                    .collect(Collectors.toList());

            if (start >= filtered.size()) {
                return List.of();
            }
            int end = (count < 0) ? filtered.size() : Math.min(start + count, filtered.size());
            return filtered.subList(start, end);
        } catch (final SubstrateSdkException e) {
            throw MulticloudjUtils.wrapAsIOException("keys", namespace, e);
        }
    }

    @Override
    public int size(final String namespace) throws IOException {
        checkSize(namespace);
        try {
            int total = MulticloudjUtils.countKeys(bucketClient, getObjectKeyPrefix(namespace));
            String markerKey = getObjectKeyPrefix(namespace) + "/" + NAMESPACE_IDENTIFIER;
            if (MulticloudjUtils.doesObjectExist(bucketClient, markerKey)) {
                total--;
            }
            return total;
        } catch (final SubstrateSdkException e) {
            throw MulticloudjUtils.wrapAsIOException("size", namespace, e);
        }
    }

    @Override
    protected String getObjectKeyPrefix(final String namespace) {
        return String.format("%s/%s", OBJECT_KEY_PREFIX, trim(namespace));
    }

    private String getObjectKey(final String namespace, final String key) {
        return String.format("%s/%s", getObjectKeyPrefix(namespace), key);
    }
}
