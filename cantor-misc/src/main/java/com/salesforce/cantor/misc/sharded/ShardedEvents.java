/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.sharded;

import com.salesforce.cantor.Events;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.EventsPreconditions.*;

public class ShardedEvents implements Events {
    private final AtomicReference<Map<String, List<Events>>> namespaceLookupTable = new AtomicReference<>();
    private final Events[] delegates;

    public ShardedEvents(final Events... delegates) {
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
    public void store(final String namespace, final Collection<Event> batch) throws IOException {
        checkStore(namespace, batch);
        getShard(namespace).store(namespace, batch);
    }

    @Override
    public List<Event> get(final String namespace,
                           final long startTimestampMillis,
                           final long endTimestampMillis,
                           final Map<String, String> metadataQuery,
                           final Map<String, String> dimensionsQuery,
                           final boolean includePayloads,
                           final boolean ascending,
                           final int limit) throws IOException {
        checkGet(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        return getShard(namespace)
                .get(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery, includePayloads, ascending, limit);
    }

    @Override
    public int delete(final String namespace,
                      final long startTimestampMillis,
                      final long endTimestampMillis,
                      final Map<String, String> metadataQuery,
                      final Map<String, String> dimensionsQuery) throws IOException {
        checkDelete(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        return getShard(namespace)
                .delete(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
    }

    @Override
    public final Map<Long, Double> aggregate(final String namespace,
                                             final String dimension,
                                             final long startTimestampMillis,
                                             final long endTimestampMillis,
                                             final Map<String, String> metadataQuery,
                                             final Map<String, String> dimensionsQuery,
                                             final int aggregateIntervalMillis,
                                             final AggregationFunction aggregationFunction) throws IOException {
        checkAggregate(namespace,
                dimension,
                startTimestampMillis,
                endTimestampMillis,
                metadataQuery,
                dimensionsQuery,
                aggregateIntervalMillis,
                aggregationFunction
        );
        return getShard(namespace)
                .aggregate(namespace,
                        dimension,
                        startTimestampMillis,
                        endTimestampMillis,
                        metadataQuery,
                        dimensionsQuery,
                        aggregateIntervalMillis,
                        aggregationFunction
                );
    }

    @Override
    public Set<String> metadata(final String namespace,
                                final String metadataKey,
                                final long startTimestampMillis,
                                final long endTimestampMillis,
                                final Map<String, String> metadataQuery,
                                final Map<String, String> dimensionsQuery) throws IOException {
        checkMetadata(namespace, metadataKey, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        return getShard(namespace)
                .metadata(namespace,
                        metadataKey,
                        startTimestampMillis,
                        endTimestampMillis,
                        metadataQuery,
                        dimensionsQuery
                );
    }

    @Override
    public void expire(final String namespace, final long endTimestampMillis) throws IOException {
        checkExpire(namespace, endTimestampMillis);
        getShard(namespace).expire(namespace, endTimestampMillis);
    }

    private Collection<String> doNamespaces() throws IOException {
        final Set<String> results = new HashSet<>();
        for (final Events delegate : this.delegates) {
            results.addAll(delegate.namespaces());
        }
        return results;
    }

    private void loadNamespaceLookupTable() throws IOException {
        final Map<String, List<Events>> namespaceToDelegates = new HashMap<>();
        for (final Events delegate : this.delegates) {
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

    private Events getShard(final String namespace) throws IOException {
        reloadLookupIfNeeded(namespace);

        final List<Events> delegates = this.namespaceLookupTable.get().get(namespace);
        if (delegates == null) {
            throw new IOException("shard not found for events namespace " + namespace);
        }
        if (delegates.size() != 1) {
            throw new IOException("more than one shard found for events namespace " + namespace);
        }
        return delegates.get(0);
    }

    private Events getShardForCreate(final String namespace) throws IOException {
        reloadLookupIfNeeded(namespace);

        // if namespace is found in the lookup table, return that
        if (this.namespaceLookupTable.get().containsKey(namespace)) {
            return getShard(namespace);
        }
        // otherwise, find the shard with smallest number of namespaces and return that
        int minNamespaceCount = this.delegates[0].namespaces().size();
        Events smallestShard = this.delegates[0];
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
