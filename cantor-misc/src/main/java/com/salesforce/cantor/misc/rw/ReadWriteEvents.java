/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.rw;

import com.salesforce.cantor.Events;

import java.io.IOException;
import java.util.*;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.EventsPreconditions.*;

public class ReadWriteEvents implements Events {
    private final Events writable;
    private final Events readable;

    public ReadWriteEvents(final Events writable, final Events readable) {
        checkArgument(writable != null, "null writable");
        checkArgument(readable != null, "null readable");
        this.writable = writable;
        this.readable = readable;
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        return this.readable.namespaces();
    }

    @Override
    public void create(final String namespace) throws IOException {
        checkCreate(namespace);
        this.writable.create(namespace);
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        this.writable.drop(namespace);
    }

    @Override
    public void store(final String namespace, final Collection<Event> batch) throws IOException {
        checkStore(namespace, batch);
        this.writable.store(namespace, batch);
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
        return this.readable.get(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery, includePayloads, ascending, limit);
    }

    @Override
    public int delete(final String namespace,
                      final long startTimestampMillis,
                      final long endTimestampMillis,
                      final Map<String, String> metadataQuery,
                      final Map<String, String> dimensionsQuery) throws IOException {
        checkDelete(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        return this.writable.delete(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
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
        return this.readable.aggregate(namespace,
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
        return this.readable.metadata(namespace,
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
        this.writable.expire(namespace, endTimestampMillis);
    }
}
