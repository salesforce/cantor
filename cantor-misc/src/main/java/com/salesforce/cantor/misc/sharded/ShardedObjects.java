/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.sharded;

import com.salesforce.cantor.Objects;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.ObjectsPreconditions.*;

public class ShardedObjects implements Objects {
    private final AtomicReference<Map<String, List<Objects>>> namespaceLookupTable = new AtomicReference<>();
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
        getShardForCreate(namespace).create(namespace);
        loadNamespaceLookupTable();
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        getShard(namespace).drop(namespace);
        loadNamespaceLookupTable(); // reload lookup table after dropping
    }

    @Override
    public void store(final String namespace, final String key, final byte[] bytes) throws IOException {
        checkStore(namespace, key, bytes);
        getShard(namespace).store(namespace, key, bytes);
    }

    @Override
    public void store(final String namespace, final Map<String, byte[]> batch) throws IOException {
        checkStore(namespace, batch);
        getShard(namespace).store(namespace, batch);
    }

    @Override
    public byte[] get(final String namespace, final String key) throws IOException {
        checkGet(namespace, key);
        return getShard(namespace).get(namespace, key);
    }

    @Override
    public Map<String, byte[]> get(final String namespace, final Collection<String> keys) throws IOException {
        checkGet(namespace, keys);
        return getShard(namespace).get(namespace, keys);
    }

    @Override
    public boolean delete(final String namespace, final String key) throws IOException {
        checkDelete(namespace, key);
        return getShard(namespace).delete(namespace, key);
    }

    @Override
    public void delete(final String namespace, final Collection<String> keys) throws IOException {
        checkDelete(namespace, keys);
        getShard(namespace).delete(namespace, keys);
    }

    @Override
    public Collection<String> keys(final String namespace, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count);
        return getShard(namespace).keys(namespace, start, count);
    }

    @Override
    public int size(final String namespace) throws IOException {
        checkSize(namespace);
        return getShard(namespace).size(namespace);
    }

    private Collection<String> doNamespaces() throws IOException {
        final Set<String> results = new HashSet<>();
        for (final Objects delegate : this.delegates) {
            results.addAll(delegate.namespaces());
        }
        return results;
    }

    private void loadNamespaceLookupTable() throws IOException {
        final Map<String, List<Objects>> namespaceToDelegates = new HashMap<>();
        for (final Objects delegate : this.delegates) {
            for (final String namespace : delegate.namespaces()) {
                namespaceToDelegates.putIfAbsent(namespace, new ArrayList<>());
                namespaceToDelegates.get(namespace).add(delegate);
            }
        }
        this.namespaceLookupTable.set(namespaceToDelegates);
    }

    private void reloadLookupIfNeeded(final String namespace) throws IOException {
        // if namespace is not found, reload the lookup table, perhaps another client has created it
        if (this.namespaceLookupTable.get() == null || !this.namespaceLookupTable.get().containsKey(namespace)) {
            loadNamespaceLookupTable();
        }
    }

    private Objects getShard(final String namespace) throws IOException {
        reloadLookupIfNeeded(namespace);

        final List<Objects> delegates = this.namespaceLookupTable.get().get(namespace);
        if (delegates == null) {
            throw new IOException("shard not found for objects namespace " + namespace);
        }
        if (delegates.size() != 1) {
            throw new IOException("more than one shard found for objects namespace " + namespace);
        }
        return delegates.get(0);
    }

    private Objects getShardForCreate(final String namespace) throws IOException {
        reloadLookupIfNeeded(namespace);

        // if namespace is found in the lookup table, return that
        if (this.namespaceLookupTable.get().containsKey(namespace)) {
            return getShard(namespace);
        }
        // otherwise, find the shard with smallest number of namespaces and return that
        int minNamespaceCount = this.delegates[0].namespaces().size();
        Objects smallestShard = this.delegates[0];
        for (int i = 1; i < this.delegates.length; ++i) {
            final int namespaceCount = this.delegates[i].namespaces().size();
            if (namespaceCount < minNamespaceCount) {
                minNamespaceCount = namespaceCount;
                smallestShard = this.delegates[i];
            }
        }
        return smallestShard;
    }
}
