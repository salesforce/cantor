/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.archivable.impl;

import com.salesforce.cantor.Events;
import com.salesforce.cantor.misc.archivable.CantorArchiver;
import com.salesforce.cantor.misc.archivable.EventsArchiver;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper class around a delegate Events instance, adding logging and time spent.
 */
public class ArchivableEvents extends AbstractBaseArchivableNamespaceable<Events, CantorArchiver> implements Events {
    public ArchivableEvents(final Events delegate, final CantorArchiver archiver) {
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
        tryRestore(namespace, startTimestampMillis, endTimestampMillis);
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
        // choosing not to archive explicitly deleted events; will not delete events already archived
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
        tryRestore(namespace, startTimestampMillis, endTimestampMillis);
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
        tryRestore(namespace, startTimestampMillis, endTimestampMillis);
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
        getArchiver().events().archive(getDelegate(),
                namespace,
                endTimestampMillis
        );

        getDelegate().expire(namespace, endTimestampMillis);
    }

    /**
     * Calling user implemented {@link EventsArchiver#hasArchives(String, long, long)} method to check if restore should
     * be executed.
     */
    private void tryRestore(final String namespace,
                            final long startTimestampMillis,
                            final long endTimestampMillis) throws IOException {
        if (getArchiver().events().hasArchives(namespace, startTimestampMillis, endTimestampMillis)) {
            getArchiver().events().restore(getDelegate(), namespace, startTimestampMillis, endTimestampMillis);
        }
    }
}
