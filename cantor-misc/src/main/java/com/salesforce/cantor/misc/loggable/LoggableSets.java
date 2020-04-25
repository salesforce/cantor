/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.loggable;

import com.salesforce.cantor.Sets;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.salesforce.cantor.common.CommonPreconditions.*;
import static com.salesforce.cantor.common.SetsPreconditions.*;

/**
 * Wrapper class around a delegate Sets instance, adding logging and time spent.
 */
public class LoggableSets extends AbstractBaseLoggableCantor implements Sets {
    private final Sets delegate;

    public LoggableSets(final Sets delegate) {
        checkArgument(delegate != null, "null delegate");
        this.delegate = delegate;
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        return logCall(this.delegate::namespaces, "namespaces", null);
    }

    @Override
    public void create(final String namespace) throws IOException {
        checkCreate(namespace);
        logCall(() -> { this.delegate.create(namespace); return null; },
                "create", namespace
        );
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        logCall(() -> { this.delegate.drop(namespace); return null; },
                "drop", namespace
        );
    }

    @Override
    public void add(final String namespace, final String set, final String entry, final long weight) throws IOException {
        checkAdd(namespace, set, entry, weight);
        logCall(
                () -> { this.delegate.add(namespace, set, entry, weight); return null; },
                "add", namespace, set, entry, weight
        );
    }

    @Override
    public void add(final String namespace, final String set, final Map<String, Long> entries) throws IOException {
        checkAdd(namespace, set, entries);
        logCall(
                () -> { this.delegate.add(namespace, set, entries); return null; },
                "add", namespace, set, entries
        );
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
        return logCall(
                () -> this.delegate.entries(namespace, set, min, max, start, count, ascending),
                "entries", namespace, set, min, max, start, count, ascending
        );
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
        return logCall(
                () -> this.delegate.get(namespace, set, min, max, start, count, ascending),
                "get", namespace, set, min, max, start, count, ascending
        );
    }

    @Override
    public void delete(final String namespace, final String set, final long min, final long max) throws IOException {
        checkDelete(namespace, set, min, max);
        logCall(
                () -> { this.delegate.delete(namespace, set, min, max); return null; },
                "delete", namespace, set, min, max
        );
    }

    @Override
    public final boolean delete(final String namespace, final String set, final String entry) throws IOException {
        checkDelete(namespace, set, entry);
        return logCall(
                () -> this.delegate.delete(namespace, set, entry),
                "delete", namespace, set, entry
        );
    }

    @Override
    public void delete(final String namespace, final String set, final Collection<String> entries) throws IOException {
        checkDelete(namespace, set, entries);
        logCall(
                () -> { this.delegate.delete(namespace, set, entries); return null; },
                "delete", namespace, set, entries
        );
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
        return logCall(
                () -> this.delegate.union(namespace, sets, min, max, start, count, ascending),
                "union", namespace, sets, min, max, start, count
        );
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
        return logCall(
                () -> this.delegate.intersect(namespace, sets, min, max, start, count, ascending),
                "intersect", namespace, sets, min, max, start, count, ascending
        );
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
        return logCall(
                () -> this.delegate.pop(namespace, set, min, max, start, count, ascending),
                "pop", namespace, set, min, max, start, count, ascending
        );
    }

    @Override
    public Collection<String> sets(final String namespace) throws IOException {
        checkSets(namespace);
        return logCall(
                () -> this.delegate.sets(namespace),
                "sets", namespace
        );
    }

    @Override
    public final int size(final String namespace, final String set) throws IOException {
        checkSize(namespace, set);
        return logCall(
                () -> this.delegate.size(namespace, set),
                "size", namespace, set
        );
    }

    @Override
    public Long weight(final String namespace, final String set, final String entry) throws IOException {
        checkWeight(namespace, set, entry);
        return logCall(
                () -> this.delegate.weight(namespace, set, entry),
                "weight", namespace, set, entry
        );
    }

    @Override
    public long inc(final String namespace, final String set, final String entry, final long count) throws IOException {
        checkInc(namespace, set, entry, count);
        return logCall(
                () -> this.delegate.inc(namespace, set, entry, count),
                "inc", namespace, set, entry, count
        );
    }
}
