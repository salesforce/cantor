/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.async;

import com.salesforce.cantor.Events;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.EventsPreconditions.*;

public class AsyncEvents extends AbstractBaseAsyncNamespaceable<Events> implements Events {
    public AsyncEvents(final Events delegate, final ExecutorService executorService) {
        super(delegate, executorService);
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        return submitCall(getDelegate()::namespaces);
    }

    @Override
    public void create(final String namespace) throws IOException {
        checkCreate(namespace);
        submitCall(() -> { getDelegate().create(namespace); return null; });
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        submitCall(() -> { getDelegate().drop(namespace); return null; });
    }

    @Override
    public void store(final String namespace, final Collection<Event> batch) throws IOException {
        checkStore(namespace, batch);
        submitCall(() -> { getDelegate().store(namespace, batch); return null; });
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
        return submitCall(() -> getDelegate()
                .get(namespace,
                        startTimestampMillis,
                        endTimestampMillis,
                        metadataQuery,
                        dimensionsQuery,
                        includePayloads,
                        ascending,
                        limit)
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
        return submitCall(() -> getDelegate()
                .metadata(namespace,
                        metadataKey,
                        startTimestampMillis,
                        endTimestampMillis,
                        metadataQuery,
                        dimensionsQuery
                )
        );
    }

    @Override
    public void expire(final String namespace, final long endTimestampMillis) throws IOException {
        checkExpire(namespace, endTimestampMillis);
        submitCall(() -> { getDelegate().expire(namespace, endTimestampMillis); return null; });
    }
}

