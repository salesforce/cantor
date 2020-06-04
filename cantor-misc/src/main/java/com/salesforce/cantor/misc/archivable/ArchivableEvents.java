/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.archivable;

import com.salesforce.cantor.Events;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper class around a delegate Events instance, adding logging and time spent.
 */
public class ArchivableEvents extends AbstractBaseArchivableNamespaceable<Events, EventsArchiver> implements Events {
    public ArchivableEvents(final Events delegate, final EventsArchiver archiver) {
        super(delegate, archiver);
    }

    @Override
    public void store(final String namespace, final Collection<Event> batch) throws IOException {
        // direct pass-through to delegate
        getDelegate().store(namespace, batch);
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
        if (getArchiveDelegate().hasArchives(getDelegate(), namespace, startTimestampMillis, endTimestampMillis)) {
            getArchiveDelegate().restore(getDelegate(), namespace, startTimestampMillis, endTimestampMillis);
        }

        return getDelegate().get(namespace,
                        startTimestampMillis,
                        endTimestampMillis,
                        metadataQuery,
                        dimensionsQuery,
                        includePayloads,
                        ascending,
                        limit
                );
    }

    @Override
    public int delete(final String namespace,
                      final long startTimestampMillis,
                      final long endTimestampMillis,
                      final Map<String, String> metadataQuery,
                      final Map<String, String> dimensionsQuery) throws IOException {
        return getDelegate().delete(namespace,
                        startTimestampMillis,
                        endTimestampMillis,
                        metadataQuery,
                        dimensionsQuery
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
        if (getArchiveDelegate().hasArchives(getDelegate(), namespace, startTimestampMillis, endTimestampMillis)) {
            getArchiveDelegate().restore(getDelegate(), namespace, startTimestampMillis, endTimestampMillis);
        }

        return getDelegate().aggregate(namespace,
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
        if (getArchiveDelegate().hasArchives(getDelegate(), namespace, startTimestampMillis, endTimestampMillis)) {
            getArchiveDelegate().restore(getDelegate(), namespace, startTimestampMillis, endTimestampMillis);
        }

        return getDelegate().metadata(namespace,
                        metadataKey,
                        startTimestampMillis,
                        endTimestampMillis,
                        metadataQuery,
                        dimensionsQuery
                );
    }

    @Override
    public void expire(final String namespace, final long endTimestampMillis) throws IOException {
        // archiving all before deletion
        getArchiveDelegate().archive(getDelegate(),
                        namespace,
                        endTimestampMillis
                );

        getDelegate().expire(namespace, endTimestampMillis);
    }
}

