/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.metrics;

import com.codahale.metrics.MetricRegistry;
import com.salesforce.cantor.Events;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetricCollectingEvents extends BaseMetricCollectingCantor implements Events {
   private final Events delegate;

    public MetricCollectingEvents(final MetricRegistry metrics, final Events delegate) {
        super(metrics, delegate);
        this.delegate = delegate;
    }

    @Override
    public void create(final String namespace) throws IOException {
        metrics(() -> this.delegate.create(namespace), "create", namespace);
    }

    @Override
    public void drop(final String namespace) throws IOException {
        metrics(() -> this.delegate.drop(namespace), "drop", namespace);
    }

    @Override
    public void store(final String namespace, final Collection<Event> batch) throws IOException {
        metrics(() -> this.delegate.store(namespace, batch), "store", namespace);
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
        return metrics(() -> this.delegate.get(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery, includePayloads, ascending, limit),
                "get", namespace, super::size);
    }

    @Override
    public Set<String> metadata(final String namespace,
                                final String metadataKey,
                                final long startTimestampMillis,
                                final long endTimestampMillis,
                                final Map<String, String> metadataQuery,
                                final Map<String, String> dimensionsQuery) throws IOException {
        return metrics(() -> this.delegate
                .metadata(namespace,
                        metadataKey,
                        startTimestampMillis,
                        endTimestampMillis,
                        metadataQuery,
                        dimensionsQuery
                ), "metadata", namespace, super::size);
    }

    @Override
    public List<Event> dimension(final String namespace,
                                 final String dimensionKey,
                                 final long startTimestampMillis,
                                 final long endTimestampMillis,
                                 final Map<String, String> metadataQuery,
                                 final Map<String, String> dimensionsQuery) throws IOException {
        return metrics(() -> this.delegate
                .dimension(namespace,
                        dimensionKey,
                        startTimestampMillis,
                        endTimestampMillis,
                        metadataQuery,
                        dimensionsQuery
                ), "dimension", namespace, super::size);
    }

    @Override
    public void expire(final String namespace, final long endTimestampMillis) throws IOException {
        metrics(() -> this.delegate.expire(namespace, endTimestampMillis), "expire", namespace);
    }
}
