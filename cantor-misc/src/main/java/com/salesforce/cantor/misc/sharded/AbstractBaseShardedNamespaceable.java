/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.sharded;

import com.salesforce.cantor.Namespaceable;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

abstract class AbstractBaseShardedNamespaceable<T extends Namespaceable> implements Namespaceable {

    private final T[] delegates;
    private final AtomicReference<Map<String, List<T>>> namespaceLookupTable = new AtomicReference<>();

    public AbstractBaseShardedNamespaceable(final T[] delegates) {
        checkArgument(delegates != null && delegates.length > 0, "null/empty delegates");
        this.delegates = delegates;
    }

    @Override
    public final Collection<String> namespaces() throws IOException {
        return doNamespaces();
    }

    protected T getShardForCreate(final String namespace) throws IOException {
        reloadLookupIfNeeded(namespace);

        // if namespace is found in the lookup table, return that
        if (this.namespaceLookupTable.get().containsKey(namespace)) {
            return getShard(namespace);
        }
        // otherwise, find the shard with smallest number of namespaces and return that
        int minNamespaceCount = this.delegates[0].namespaces().size();
        T smallestShard = this.delegates[0];
        for (int i = 1; i < this.delegates.length; ++i) {
            final int namespaceCount = this.delegates[i].namespaces().size();
            if (namespaceCount < minNamespaceCount) {
                minNamespaceCount = namespaceCount;
                smallestShard = this.delegates[i];
            }
        }
        return smallestShard;
    }

    protected T getShard(final String namespace) throws IOException {
        reloadLookupIfNeeded(namespace);

        final List<T> shards = this.namespaceLookupTable.get().get(namespace);
        if (shards == null) {
            throw new IOException("shard not found for namespace " + namespace);
        }
        if (shards.size() != 1) {
            throw new IOException("more than one shard found for namespace " + namespace);
        }
        return shards.get(0);
    }

    protected void reloadLookupIfNeeded(final String namespace) throws IOException {
        // if namespace is not found, reload the lookup table, perhaps another client has created it
        if (this.namespaceLookupTable.get() == null || !this.namespaceLookupTable.get().containsKey(namespace)) {
            loadNamespaceLookupTable();
        }
    }

    protected void loadNamespaceLookupTable() throws IOException {
        final Map<String, List<T>> namespaceToDelegates = new HashMap<>();
        for (final T delegate : this.delegates) {
            for (final String namespace : delegate.namespaces()) {
                namespaceToDelegates.putIfAbsent(namespace, new ArrayList<>());
                namespaceToDelegates.get(namespace).add(delegate);
            }
        }
        this.namespaceLookupTable.set(namespaceToDelegates);
    }

    private Collection<String> doNamespaces() throws IOException {
        final Set<String> results = new HashSet<>();
        for (final T delegate : this.delegates) {
            results.addAll(delegate.namespaces());
        }
        return results;
    }
}
