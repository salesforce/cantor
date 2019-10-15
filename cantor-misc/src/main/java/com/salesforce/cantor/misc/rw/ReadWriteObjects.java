/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.rw;

import com.salesforce.cantor.Objects;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.ObjectsPreconditions.*;

public class ReadWriteObjects implements Objects {
    private final Objects writable;
    private final Objects readable;

    public ReadWriteObjects(final Objects writable, final Objects readable) {
        checkArgument(writable != null, "null writable");
        checkArgument(readable != null, "null readable");
        this.writable = writable;
        this.readable = readable;
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        return this.readable.namespaces();
    }

    @Override
    public void create(final String namespace) throws IOException {
        checkCreate(namespace);
        this.writable.create(namespace);
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        this.writable.drop(namespace);
    }

    @Override
    public void store(final String namespace, final String key, final byte[] bytes) throws IOException {
        checkStore(namespace, key, bytes);
        this.writable.store(namespace, key, bytes);
    }

    @Override
    public void store(final String namespace, final Map<String, byte[]> batch) throws IOException {
        checkStore(namespace, batch);
        this.writable.store(namespace, batch);
    }

    @Override
    public byte[] get(final String namespace, final String key) throws IOException {
        checkGet(namespace, key);
        return this.readable.get(namespace, key);
    }

    @Override
    public Map<String, byte[]> get(final String namespace, final Collection<String> keys) throws IOException {
        checkGet(namespace, keys);
        return this.readable.get(namespace, keys);
    }

    @Override
    public boolean delete(final String namespace, final String key) throws IOException {
        checkDelete(namespace, key);
        return this.writable.delete(namespace, key);
    }

    @Override
    public void delete(final String namespace, final Collection<String> keys) throws IOException {
        checkDelete(namespace, keys);
        this.writable.delete(namespace, keys);
    }

    @Override
    public Collection<String> keys(final String namespace, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count);
        return this.readable.keys(namespace, start, count);
    }

    @Override
    public int size(final String namespace) throws IOException {
        checkSize(namespace);
        return this.readable.size(namespace);
    }
}
