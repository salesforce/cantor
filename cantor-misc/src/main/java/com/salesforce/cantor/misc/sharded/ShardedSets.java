/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.sharded;

import com.salesforce.cantor.Sets;

import java.io.IOException;
import java.util.*;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.SetsPreconditions.*;

public class ShardedSets extends AbstractBaseShardedNamespaceable<Sets> implements Sets {
    public ShardedSets(final Sets... delegates) {
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
    public void add(final String namespace, final String set, final String entry, final long weight) throws IOException {
        checkAdd(namespace, set, entry, weight);
        getShard(namespace).add(namespace, set, entry, weight);
    }

    @Override
    public void add(final String namespace, final String set, final Map<String, Long> entries) throws IOException {
        checkAdd(namespace, set, entries);
        getShard(namespace).add(namespace, set, entries);
    }

    @Override
    public Collection<String> entries(final String namespace,
                                      final String set,
                                      final long min,
                                      final long max,
                                      final int start,
                                      final int count,
                                      final boolean ascending) throws IOException {
        checkEntries(namespace, set, min, max, start, count, ascending);
        return getShard(namespace).entries(namespace, set, min, max, start, count, ascending);
    }

    @Override
    public Map<String, Long> get(final String namespace,
                                 final String set,
                                 final long min,
                                 final long max,
                                 final int start,
                                 final int count,
                                 final boolean ascending) throws IOException {
        checkGet(namespace, set, min, max, start, count, ascending);
        return getShard(namespace).get(namespace, set, min, max, start, count, ascending);
    }

    @Override
    public void delete(final String namespace, final String set, final long min, final long max) throws IOException {
        checkDelete(namespace, set, min, max);
        getShard(namespace).delete(namespace, set, min, max);
    }

    @Override
    public final boolean delete(final String namespace, final String set, final String entry) throws IOException {
        checkDelete(namespace, set, entry);
        return getShard(namespace).delete(namespace, set, entry);
    }

    @Override
    public void delete(final String namespace, final String set, final Collection<String> entries) throws IOException {
        checkDelete(namespace, set, entries);
        getShard(namespace).delete(namespace, set, entries);
    }

    @Override
    public Map<String, Long> union(final String namespace,
                                   final Collection<String> sets,
                                   final long min,
                                   final long max,
                                   final int start,
                                   final int count,
                                   final boolean ascending) throws IOException {
        checkUnion(namespace, sets, min, max, start, count, ascending);
        return getShard(namespace).union(namespace, sets, min, max, start, count, ascending);
    }

    @Override
    public Map<String, Long> intersect(final String namespace,
                                       final Collection<String> sets,
                                       final long min,
                                       final long max,
                                       final int start,
                                       final int count,
                                       final boolean ascending) throws IOException {
        checkIntersect(namespace, sets, min, max, start, count, ascending);
        return getShard(namespace).intersect(namespace, sets, min, max, start, count, ascending);
    }

    @Override
    public Map<String, Long> pop(final String namespace,
                                 final String set,
                                 final long min,
                                 final long max,
                                 final int start,
                                 final int count,
                                 final boolean ascending) throws IOException {
        checkPop(namespace, set, min, max, start, count, ascending);
        return getShard(namespace).pop(namespace, set, min, max, start, count, ascending);
    }

    @Override
    public Collection<String> sets(final String namespace) throws IOException {
        checkSets(namespace);
        return getShard(namespace).sets(namespace);
    }

    @Override
    public final int size(final String namespace, final String set) throws IOException {
        checkSize(namespace, set);
        return getShard(namespace).size(namespace, set);
    }

    @Override
    public Long weight(final String namespace, final String set, final String entry) throws IOException {
        checkWeight(namespace, set, entry);
        return getShard(namespace).weight(namespace, set, entry);
    }

    @Override
    public long inc(final String namespace, final String set, final String entry, final long count) throws IOException {
        checkInc(namespace, set, entry, count);
        return getShard(namespace).inc(namespace, set, entry, count);
    }
}
