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

public class AsyncObjects extends AbstractBaseAsyncNamespaceable<Objects> implements Objects {
    public AsyncObjects(final Objects delegate, final ExecutorService executorService) {
        super(delegate, executorService);
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
    public void store(final String namespace, final String key, final byte[] bytes) throws IOException {
        checkStore(namespace, key, bytes);
        getDelegate().store(namespace, key, bytes);
        submitCall(() -> { getDelegate().create(namespace); return null; });
    }

    @Override
    public void store(final String namespace, final Map<String, byte[]> batch) throws IOException {
        checkStore(namespace, batch);
        submitCall(() -> { getDelegate().store(namespace, batch); return null; });
    }

    @Override
    public byte[] get(final String namespace, final String key) throws IOException {
        checkGet(namespace, key);
        return submitCall(() -> getDelegate().get(namespace, key));
    }

    @Override
    public Map<String, byte[]> get(final String namespace, final Collection<String> keys) throws IOException {
        checkGet(namespace, keys);
        return submitCall(() -> getDelegate().get(namespace, keys));
    }

    @Override
    public boolean delete(final String namespace, final String key) throws IOException {
        checkDelete(namespace, key);
        return submitCall(() -> getDelegate().delete(namespace, key));
    }

    @Override
    public void delete(final String namespace, final Collection<String> keys) throws IOException {
        checkDelete(namespace, keys);
        submitCall(() -> { getDelegate().delete(namespace, keys); return null; });
    }

    @Override
    public Collection<String> keys(final String namespace, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count);
        return submitCall(() -> getDelegate().keys(namespace, start, count));
    }

    @Override
    public Collection<String> keys(final String namespace, final String prefix, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count, prefix);
        return submitCall(() -> getDelegate().keys(namespace, prefix, start, count));
    }

    @Override
    public int size(final String namespace) throws IOException {
        checkSize(namespace);
        return submitCall(() -> getDelegate().size(namespace));
    }
}
