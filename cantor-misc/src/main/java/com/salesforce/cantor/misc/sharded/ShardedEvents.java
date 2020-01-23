/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.sharded;

import com.salesforce.cantor.Events;

import java.io.IOException;
import java.util.*;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.EventsPreconditions.*;

public class ShardedEvents implements Events {
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
        getEvents(namespace).create(namespace);
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        getEvents(namespace).drop(namespace);
    }

    @Override
    public void store(final String namespace, final Collection<Event> batch) throws IOException {
        checkStore(namespace, batch);
        getEvents(namespace).store(namespace, batch);
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
        return getEvents(namespace)
                .get(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery, includePayloads, ascending, limit);
    }

    @Override
    public int delete(final String namespace,
                      final long startTimestampMillis,
                      final long endTimestampMillis,
                      final Map<String, String> metadataQuery,
                      final Map<String, String> dimensionsQuery) throws IOException {
        checkDelete(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        return getEvents(namespace)
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
        return getEvents(namespace)
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
        return getEvents(namespace)
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
        getEvents(namespace).expire(namespace, endTimestampMillis);
    }

    private Collection<String> doNamespaces() throws IOException {
        final List<String> results = new ArrayList<>();
        for (final Events delegate : this.delegates) {
            results.addAll(delegate.namespaces());
        }
        return results;
    }

    private Events getEvents(final String namespace) {
        return Shardeds.getShard(this.delegates, namespace);
    }
}
