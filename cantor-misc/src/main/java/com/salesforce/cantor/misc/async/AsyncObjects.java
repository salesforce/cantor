/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.async;

import com.salesforce.cantor.Objects;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.ObjectsPreconditions.*;

public class AsyncObjects extends AbstractBaseAsyncCantor implements Objects {
    private final Objects delegate;

    public AsyncObjects(final Objects delegate, final ExecutorService executorService) {
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
    public void store(final String namespace, final String key, final byte[] bytes) throws IOException {
        checkStore(namespace, key, bytes);
        this.delegate.store(namespace, key, bytes);
        submitCall(() -> { this.delegate.create(namespace); return null; });
    }

    @Override
    public void store(final String namespace, final Map<String, byte[]> batch) throws IOException {
        checkStore(namespace, batch);
        submitCall(() -> { this.delegate.store(namespace, batch); return null; });
    }

    @Override
    public byte[] get(final String namespace, final String key) throws IOException {
        checkGet(namespace, key);
        return submitCall(() -> this.delegate.get(namespace, key));
    }

    @Override
    public Map<String, byte[]> get(final String namespace, final Collection<String> keys) throws IOException {
        checkGet(namespace, keys);
        return submitCall(() -> this.delegate.get(namespace, keys));
    }

    @Override
    public boolean delete(final String namespace, final String key) throws IOException {
        checkDelete(namespace, key);
        return submitCall(() -> this.delegate.delete(namespace, key));
    }

    @Override
    public void delete(final String namespace, final Collection<String> keys) throws IOException {
        checkDelete(namespace, keys);
        submitCall(() -> { this.delegate.delete(namespace, keys); return null; });
    }

    @Override
    public Collection<String> keys(final String namespace, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count);
        return submitCall(() -> this.delegate.keys(namespace, start, count));
    }

    @Override
    public int size(final String namespace) throws IOException {
        checkSize(namespace);
        return submitCall(() -> this.delegate.size(namespace));
    }
}
