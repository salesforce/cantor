/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.loggable;

import com.salesforce.cantor.Objects;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.salesforce.cantor.common.ObjectsPreconditions.*;

/**
 * Wrapper class around a delegate Objects instance, adding logging and time spent.
 */
public class LoggableObjects extends AbstractBaseLoggableNamespaceable<Objects> implements Objects {
    public LoggableObjects(final Objects delegate) {
        super(delegate);
    }

    @Override
    public void store(final String namespace, final String key, final byte[] bytes) throws IOException {
        checkStore(namespace, key, bytes);
        getDelegate().store(namespace, key, bytes);
        logCall(() -> { getDelegate().create(namespace); return null; },
                "store", namespace
        );
    }

    @Override
    public void store(final String namespace, final Map<String, byte[]> batch) throws IOException {
        checkStore(namespace, batch);
        logCall(() -> { getDelegate().store(namespace, batch); return null; },
                "store", namespace, batch.keySet()
        );
    }

    @Override
    public byte[] get(final String namespace, final String key) throws IOException {
        checkGet(namespace, key);
        return logCall(() -> getDelegate().get(namespace, key),
                "get", namespace, key
        );
    }

    @Override
    public Map<String, byte[]> get(final String namespace, final Collection<String> keys) throws IOException {
        checkGet(namespace, keys);
        return logCall(() -> getDelegate().get(namespace, keys),
                "get", namespace, keys
        );
    }

    @Override
    public boolean delete(final String namespace, final String key) throws IOException {
        checkDelete(namespace, key);
        return logCall(() -> getDelegate().delete(namespace, key),
                "delete", namespace, key
        );
    }

    @Override
    public void delete(final String namespace, final Collection<String> keys) throws IOException {
        checkDelete(namespace, keys);
        logCall(() -> { getDelegate().delete(namespace, keys); return null; },
                "delete", namespace, keys
        );
    }

    @Override
    public Collection<String> keys(final String namespace, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count);
        return logCall(() -> getDelegate().keys(namespace, start, count),
                "keys", namespace, start, count
        );
    }

    @Override
    public Collection<String> keys(final String namespace, final String prefix, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count, prefix);
        return logCall(() -> getDelegate().keys(namespace, prefix, start, count),
                "keys", namespace, start, count
        );
    }

    @Override
    public int size(final String namespace) throws IOException {
        checkSize(namespace);
        return logCall(() -> getDelegate().size(namespace),
                "size", namespace
        );
    }
}
