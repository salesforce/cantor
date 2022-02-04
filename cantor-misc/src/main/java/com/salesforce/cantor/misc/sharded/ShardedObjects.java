/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.sharded;

import com.salesforce.cantor.Objects;

import java.io.IOException;
import java.util.*;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.ObjectsPreconditions.*;

public class ShardedObjects extends AbstractBaseShardedNamespaceable<Objects> implements Objects {
    public ShardedObjects(final Objects... delegates) {
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
    public void store(final String namespace, final String key, final byte[] bytes) throws IOException {
        checkStore(namespace, key, bytes);
        getShard(namespace).store(namespace, key, bytes);
    }

    @Override
    public void store(final String namespace, final Map<String, byte[]> batch) throws IOException {
        checkStore(namespace, batch);
        getShard(namespace).store(namespace, batch);
    }

    @Override
    public byte[] get(final String namespace, final String key) throws IOException {
        checkGet(namespace, key);
        return getShard(namespace).get(namespace, key);
    }

    @Override
    public Map<String, byte[]> get(final String namespace, final Collection<String> keys) throws IOException {
        checkGet(namespace, keys);
        return getShard(namespace).get(namespace, keys);
    }

    @Override
    public boolean delete(final String namespace, final String key) throws IOException {
        checkDelete(namespace, key);
        return getShard(namespace).delete(namespace, key);
    }

    @Override
    public void delete(final String namespace, final Collection<String> keys) throws IOException {
        checkDelete(namespace, keys);
        getShard(namespace).delete(namespace, keys);
    }

    @Override
    public Collection<String> keys(final String namespace, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count);
        return getShard(namespace).keys(namespace, start, count);
    }

    @Override
    public Collection<String> keys(final String namespace, final String prefix, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count, prefix);
        return getShard(namespace).keys(namespace, prefix, start, count);
    }

    @Override
    public int size(final String namespace) throws IOException {
        checkSize(namespace);
        return getShard(namespace).size(namespace);
    }
}
