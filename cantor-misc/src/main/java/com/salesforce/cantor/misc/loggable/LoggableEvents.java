/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.loggable;

import com.salesforce.cantor.Events;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.CommonUtils.nullToEmpty;
import static com.salesforce.cantor.common.EventsPreconditions.*;

/**
 * Wrapper class around a delegate Events instance, adding logging and time spent.
 */
public class LoggableEvents extends AbstractBaseLoggableCantor implements Events {
    private final Events delegate;

    public LoggableEvents(final Events delegate) {
        checkArgument(delegate != null, "null delegate");
        this.delegate = delegate;
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        return logCall(this.delegate::namespaces, "namespaces", null);
    }

    @Override
    public void create(final String namespace) throws IOException {
        checkCreate(namespace);
        logCall(() -> { this.delegate.create(namespace); return null; },
                "create", namespace
        );
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        logCall(() -> { this.delegate.drop(namespace); return null; },
                "drop", namespace
        );
    }

    @Override
    public void store(final String namespace, final Collection<Event> batch) throws IOException {
        checkStore(namespace, batch);
        logCall(() -> { this.delegate.store(namespace, batch); return null; },
                "store", namespace, batch.size()
        );
    }

    @Override
    public List<Event> get(final String namespace,
                           final long startTimestampMillis,
                           final long endTimestampMillis,
                           final Map<String, String> metadataQuery,
                           final Map<String, String> dimensionsQuery,
                           final boolean includePayloads) throws IOException {
        checkGet(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        return logCall(() -> this.delegate.get(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery, includePayloads),
                "get", namespace,
                startTimestampMillis, endTimestampMillis,
                nullToEmpty(metadataQuery).keySet(), nullToEmpty(dimensionsQuery).keySet(),
                includePayloads
        );
    }

    @Override
    public int delete(final String namespace,
                      final long startTimestampMillis,
                      final long endTimestampMillis,
                      final Map<String, String> metadataQuery,
                      final Map<String, String> dimensionsQuery) throws IOException {
        checkDelete(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        return logCall(() -> this.delegate.delete(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery),
                "delete", namespace,
                startTimestampMillis, endTimestampMillis,
                nullToEmpty(metadataQuery).keySet(), nullToEmpty(dimensionsQuery).keySet()
        );
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
        return logCall(() -> this.delegate
                .aggregate(namespace,
                        dimension,
                        startTimestampMillis,
                        endTimestampMillis,
                        metadataQuery,
                        dimensionsQuery,
                        aggregateIntervalMillis,
                        aggregationFunction
                ),
                "aggregate", namespace,
                dimension, startTimestampMillis, endTimestampMillis,
                nullToEmpty(metadataQuery).keySet(), dimension,
                aggregateIntervalMillis, aggregationFunction
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
        return logCall(() -> this.delegate
                .metadata(namespace,
                        metadataKey,
                        startTimestampMillis,
                        endTimestampMillis,
                        metadataQuery,
                        dimensionsQuery
                ),
                "metadata", namespace,
                metadataKey, startTimestampMillis, endTimestampMillis,
                nullToEmpty(metadataQuery).keySet(), nullToEmpty(dimensionsQuery).keySet()
        );
    }

    @Override
    public void expire(final String namespace, final long endTimestampMillis) throws IOException {
        checkExpire(namespace, endTimestampMillis);
        logCall(() -> { this.delegate.expire(namespace, endTimestampMillis); return null; },
                "expire", namespace, endTimestampMillis
        );
    }
}

