/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.rw;

import com.salesforce.cantor.Objects;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.salesforce.cantor.common.ObjectsPreconditions.*;

public class ReadWriteObjects extends AbstractBaseReadWriteNamespaceable<Objects> implements Objects {
    public ReadWriteObjects(final Objects writable, final Objects readable) {
        super(writable, readable);
    }

    @Override
    public void store(final String namespace, final String key, final byte[] bytes) throws IOException {
        checkStore(namespace, key, bytes);
        getWritable().store(namespace, key, bytes);
    }

    @Override
    public void store(final String namespace, final Map<String, byte[]> batch) throws IOException {
        checkStore(namespace, batch);
        getWritable().store(namespace, batch);
    }

    @Override
    public byte[] get(final String namespace, final String key) throws IOException {
        checkGet(namespace, key);
        return getReadable().get(namespace, key);
    }

    @Override
    public Map<String, byte[]> get(final String namespace, final Collection<String> keys) throws IOException {
        checkGet(namespace, keys);
        return getReadable().get(namespace, keys);
    }

    @Override
    public boolean delete(final String namespace, final String key) throws IOException {
        checkDelete(namespace, key);
        return getWritable().delete(namespace, key);
    }

    @Override
    public void delete(final String namespace, final Collection<String> keys) throws IOException {
        checkDelete(namespace, keys);
        getWritable().delete(namespace, keys);
    }

    @Override
    public Collection<String> keys(final String namespace, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count);
        return getReadable().keys(namespace, start, count);
    }

    @Override
    public Collection<String> keys(final String namespace, final String prefix, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count, prefix);
        return getReadable().keys(namespace, prefix, start, count);
    }

    @Override
    public int size(final String namespace) throws IOException {
        checkSize(namespace);
        return getReadable().size(namespace);
    }
}
