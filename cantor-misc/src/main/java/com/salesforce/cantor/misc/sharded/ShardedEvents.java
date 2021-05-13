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

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.EventsPreconditions.*;

public class ShardedEvents extends AbstractBaseShardedNamespaceable<Events> implements Events {
    public ShardedEvents(final Events... delegates) {
        super(delegates);
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
    public List<Event> dimension(final String namespace,
                                final String dimensionKey,
                                final long startTimestampMillis,
                                final long endTimestampMillis,
                                final Map<String, String> metadataQuery,
                                final Map<String, String> dimensionsQuery) throws IOException {
        checkDimension(namespace, dimensionKey, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        return getShard(namespace)
                .dimension(namespace,
                        dimensionKey,
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
}
