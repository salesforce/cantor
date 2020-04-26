/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.async;

import com.salesforce.cantor.Sets;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.SetsPreconditions.*;

public class AsyncSets extends AbstractBaseAsyncNamespaceable<Sets> implements Sets {
    public AsyncSets(final Sets delegate, final ExecutorService executorService) {
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
    public void add(final String namespace, final String set, final String entry, final long weight) throws IOException {
        checkAdd(namespace, set, entry, weight);
        submitCall(() -> { getDelegate().add(namespace, set, entry, weight); return null; });
    }

    @Override
    public void add(final String namespace, final String set, final Map<String, Long> entries) throws IOException {
        checkAdd(namespace, set, entries);
        submitCall(() -> { getDelegate().add(namespace, set, entries); return null; });
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
        return submitCall(() -> getDelegate().entries(namespace, set, min, max, start, count, ascending));
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
        return submitCall(() -> getDelegate().get(namespace, set, min, max, start, count, ascending));
    }

    @Override
    public void delete(final String namespace, final String set, final long min, final long max) throws IOException {
        checkDelete(namespace, set, min, max);
        submitCall(() -> { getDelegate().delete(namespace, set, min, max); return null; });
    }

    @Override
    public final boolean delete(final String namespace, final String set, final String entry) throws IOException {
        checkDelete(namespace, set, entry);
        return submitCall(() -> getDelegate().delete(namespace, set, entry));
    }

    @Override
    public void delete(final String namespace, final String set, final Collection<String> entries) throws IOException {
        checkDelete(namespace, set, entries);
        submitCall(() -> { getDelegate().delete(namespace, set, entries); return null; });
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
        return submitCall(() -> getDelegate().union(namespace, sets, min, max, start, count, ascending));
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
        return submitCall(() -> getDelegate().intersect(namespace, sets, min, max, start, count, ascending));
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
        return submitCall(() -> getDelegate().pop(namespace, set, min, max, start, count, ascending));
    }

    @Override
    public Collection<String> sets(final String namespace) throws IOException {
        checkSets(namespace);
        return submitCall(() -> getDelegate().sets(namespace));
    }

    @Override
    public final int size(final String namespace, final String set) throws IOException {
        checkSize(namespace, set);
        return submitCall(() -> getDelegate().size(namespace, set));
    }

    @Override
    public Long weight(final String namespace, final String set, final String entry) throws IOException {
        checkWeight(namespace, set, entry);
        return submitCall(() -> getDelegate().weight(namespace, set, entry));
    }

    @Override
    public long inc(final String namespace, final String set, final String entry, final long count) throws IOException {
        checkInc(namespace, set, entry, count);
        return submitCall(() -> getDelegate().inc(namespace, set, entry, count));
    }
}
