/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.async;

import com.salesforce.cantor.Maps;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.MapsPreconditions.*;

public class AsyncMaps extends AbstractBaseAsyncCantor implements Maps {
    private final Maps delegate;

    public AsyncMaps(final Maps delegate, final ExecutorService executorService) {
        super(executorService);
        checkArgument(delegate != null, "null delegate");
        this.delegate = delegate;
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        return submitCall(this.delegate::namespaces);
    }

    @Override
    public void create(final String namespace) throws IOException {
        checkCreate(namespace);
        submitCall(() -> { this.delegate.create(namespace); return null; });
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        submitCall(() -> { this.delegate.drop(namespace); return null; });
    }

    @Override
    public void store(final String namespace, final Map<String, String> map) throws IOException {
        checkStore(namespace, map);
        submitCall(() -> { this.delegate.store(namespace, map); return null; });
    }

    @Override
    public Collection<Map<String, String>> get(final String namespace, final Map<String, String> query) throws IOException {
        checkGet(namespace, query);
        return submitCall(() -> this.delegate.get(namespace, query));
    }

    @Override
    public int delete(final String namespace, final Map<String, String> query) throws IOException {
        checkDelete(namespace, query);
        return submitCall(() -> this.delegate.delete(namespace, query));
    }
}

