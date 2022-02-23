/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.metrics;

import com.codahale.metrics.MetricRegistry;
import com.salesforce.cantor.Sets;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

public class MetricCollectingSets extends BaseMetricCollectingCantor implements Sets {
    private final Sets delegate;

    public MetricCollectingSets(final MetricRegistry metrics, final Sets delegate) {
        super(metrics, delegate);
        this.delegate = delegate;
    }

    @Override
    public void create(final String namespace) throws IOException {
        metrics(() -> this.delegate.create(namespace), "create", namespace);
    }

    @Override
    public void drop(final String namespace) throws IOException {
        metrics(() -> this.delegate.drop(namespace), "drop", namespace);
    }

    @Override
    public void add(final String namespace, final String set, final String entry, final long weight) throws IOException {
        metrics(() -> this.delegate.add(namespace, set, entry, weight), "add", namespace);
    }

    @Override
    public void add(final String namespace, final String set, final Map<String, Long> entries) throws IOException {
        metrics(() -> this.delegate.add(namespace, set, entries), "add", namespace);
    }

    @Override
    public Collection<String> entries(final String namespace,
                                      final String set,
                                      final long min,
                                      final long max,
                                      final int start,
                                      final int count,
                                      final boolean ascending) throws IOException {
        return metrics(() -> this.delegate.entries(namespace, set, min, max, start, count, ascending),
                "entries", namespace, super::size);
    }

    @Override
    public Map<String, Long> get(final String namespace, final String set, final long min, final long max, final int start, final int count, final boolean ascending) throws IOException {
        return metrics(() -> this.delegate.get(namespace, set, min, max, start, count, ascending),
                "get", namespace, m -> m != null ? m.size() : 0);
    }

    @Override
    public void delete(final String namespace, final String set, final long min, final long max) throws IOException {
        metrics(() -> this.delegate.delete(namespace, set, min, max), "delete", namespace);
    }

    @Override
    public boolean delete(final String namespace, final String set, final String entry) throws IOException {
        return metrics(() -> this.delegate.delete(namespace, set, entry), "delete", namespace, bool -> bool  ? 1 : 0);
    }

    @Override
    public void delete(final String namespace, final String set, final Collection<String> entries) throws IOException {
        metrics(() -> this.delegate.delete(namespace, set, entries), "delete", namespace);
    }

    @Override
    public Map<String, Long> union(final String namespace, final Collection<String> sets, final long min, final long max, final int start, final int count, final boolean ascending) throws IOException {
        return metrics(() -> this.delegate.union(namespace, sets, min, max, start, count, ascending),
                "union", namespace, m -> m != null ? m.size() : 0);
    }

    @Override
    public Map<String, Long> intersect(final String namespace, final Collection<String> sets, final long min, final long max, final int start, final int count, final boolean ascending) throws IOException {
        return metrics(() -> this.delegate.intersect(namespace, sets, min, max, start, count, ascending),
                "intersect", namespace, m -> m != null ? m.size() : 0);
    }

    @Override
    public Map<String, Long> pop(final String namespace, final String set, final long min, final long max, final int start, final int count, final boolean ascending) throws IOException {
        return metrics(() -> this.delegate.pop(namespace, set, min, max, start, count, ascending),
                "pop", namespace, m -> m != null ? m.size() : 0);
    }

    @Override
    public Collection<String> sets(final String namespace) throws IOException {
        return metrics(() -> this.delegate.sets(namespace), "sets", namespace, super::size);
    }

    @Override
    public int size(final String namespace, final String set) throws IOException {
        return metrics(() -> this.delegate.size(namespace, set), "size", namespace, Function.identity());
    }

    @Override
    public Long weight(final String namespace, final String set, final String entry) throws IOException {
        // can't use the histogram function in super, this is the _only_ method that returns a long result...
        return metrics(() -> this.delegate.weight(namespace, set, entry), "weight", namespace);
    }

    @Override
    public long inc(final String namespace, final String set, final String entry, final long count) throws IOException {
        return metrics(() -> this.delegate.inc(namespace, set, entry, count), "inc", namespace);
    }
}
