/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.sharded;

import com.salesforce.cantor.Maps;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.MapsPreconditions.*;

public class ShardedMaps implements Maps {
    private final AtomicReference<Map<String, List<Maps>>> namespaceLookupTable = new AtomicReference<>();
    private final Maps[] delegates;

    public ShardedMaps(final Maps... delegates) {
        checkArgument(delegates != null && delegates.length > 0, "null/empty delegates");
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
    public void store(final String namespace, final Map<String, String> map) throws IOException {
        checkStore(namespace, map);
        getShard(namespace).store(namespace, map);
    }

    @Override
    public Collection<Map<String, String>> get(final String namespace, final Map<String, String> query) throws IOException {
        checkGet(namespace, query);
        return getShard(namespace).get(namespace, query);
    }

    @Override
    public int delete(final String namespace, final Map<String, String> query) throws IOException {
        checkDelete(namespace, query);
        return getShard(namespace).delete(namespace, query);
    }

    private Collection<String> doNamespaces() throws IOException {
        final Set<String> results = new HashSet<>();
        for (final Maps delegate : this.delegates) {
            results.addAll(delegate.namespaces());
        }
        return results;
    }

    private void loadNamespaceLookupTable() throws IOException {
        final Map<String, List<Maps>> namespaceToDelegates = new HashMap<>();
        for (final Maps delegate : this.delegates) {
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

    private Maps getShard(final String namespace) throws IOException {
        reloadLookupIfNeeded(namespace);

        final List<Maps> delegates = this.namespaceLookupTable.get().get(namespace);
        if (delegates == null) {
            throw new IOException("shard not found for maps namespace " + namespace);
        }
        if (delegates.size() != 1) {
            throw new IOException("more than one shard found for maps namespace " + namespace);
        }
        return delegates.get(0);
    }

    private Maps getShardForCreate(final String namespace) throws IOException {
        reloadLookupIfNeeded(namespace);

        // if namespace is found in the lookup table, return that
        if (this.namespaceLookupTable.get().containsKey(namespace)) {
            return getShard(namespace);
        }
        // otherwise, find the shard with smallest number of namespaces and return that
        int minNamespaceCount = this.delegates[0].namespaces().size();
        Maps smallestShard = this.delegates[0];
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
