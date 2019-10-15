/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.sharded;

import com.salesforce.cantor.Objects;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.ObjectsPreconditions.*;

public class ShardedObjects implements Objects {
    private final Objects[] delegates;

    public ShardedObjects(final Objects... delegates) {
        checkArgument(delegates != null && delegates.length > 0, "null empty/delegates");
        this.delegates = delegates;
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        return doNamespaces();
    }

    @Override
    public void create(final String namespace) throws IOException {
        checkCreate(namespace);
        getObjects(namespace).create(namespace);
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        getObjects(namespace).drop(namespace);
    }

    @Override
    public void store(final String namespace, final String key, final byte[] bytes) throws IOException {
        checkStore(namespace, key, bytes);
        getObjects(namespace).store(namespace, key, bytes);
    }

    @Override
    public void store(final String namespace, final Map<String, byte[]> batch) throws IOException {
        checkStore(namespace, batch);
        getObjects(namespace).store(namespace, batch);
    }

    @Override
    public byte[] get(final String namespace, final String key) throws IOException {
        checkGet(namespace, key);
        return getObjects(namespace).get(namespace, key);
    }

    @Override
    public Map<String, byte[]> get(final String namespace, final Collection<String> keys) throws IOException {
        checkGet(namespace, keys);
        return getObjects(namespace).get(namespace, keys);
    }

    @Override
    public boolean delete(final String namespace, final String key) throws IOException {
        checkDelete(namespace, key);
        return getObjects(namespace).delete(namespace, key);
    }

    @Override
    public void delete(final String namespace, final Collection<String> keys) throws IOException {
        checkDelete(namespace, keys);
        getObjects(namespace).delete(namespace, keys);
    }

    @Override
    public Collection<String> keys(final String namespace, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count);
        return getObjects(namespace).keys(namespace, start, count);
    }

    @Override
    public int size(final String namespace) throws IOException {
        checkSize(namespace);
        return getObjects(namespace).size(namespace);
    }

    private Collection<String> doNamespaces() throws IOException {
        final List<String> results = new ArrayList<>();
        for (final Objects delegate : this.delegates) {
            results.addAll(delegate.namespaces());
        }
        return results;
    }

    private Objects getObjects(final String namespace) {
        return Shardeds.getShard(this.delegates, namespace);
    }
}
