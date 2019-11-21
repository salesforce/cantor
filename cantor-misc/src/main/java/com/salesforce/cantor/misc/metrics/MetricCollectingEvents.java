package com.salesforce.cantor.misc.metrics;

import com.codahale.metrics.MetricRegistry;
import com.salesforce.cantor.Events;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class MetricCollectingEvents extends BaseMetricCollectingCantor implements Events {

   private final Events delegate;

    public MetricCollectingEvents(final MetricRegistry metrics, final Events delegate) {
        super(metrics, delegate);
        this.delegate = delegate;
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        return metrics(this.delegate::namespaces, "namespaces", "cantor", Collection::size);
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
    public void store(final String namespace, final Collection<Event> batch) throws IOException {
        metrics(() -> this.delegate.store(namespace, batch), "store", namespace);
    }

    @Override
    public List<Event> get(final String namespace,
                           final long startTimestampMillis,
                           final long endTimestampMillis,
                           final Map<String, String> metadataQuery,
                           final Map<String, String> dimensionsQuery,
                           final boolean includePayloads) throws IOException {
        return metrics(() -> this.delegate.get(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery, includePayloads),
                "get", namespace, List::size);
    }

    @Override
    public int delete(final String namespace,
                      final long startTimestampMillis,
                      final long endTimestampMillis,
                      final Map<String, String> metadataQuery,
                      final Map<String, String> dimensionsQuery) throws IOException {
        return metrics(() -> this.delegate.delete(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery),
                "delete", namespace, Function.identity());
    }

    @Override
    public Map<Long, Double> aggregate(final String namespace,
                                       final String dimension,
                                       final long startTimestampMillis,
                                       final long endTimestampMillis,
                                       final Map<String, String> metadataQuery,
                                       final Map<String, String> dimensionsQuery,
                                       final int aggregateIntervalMillis,
                                       final AggregationFunction aggregationFunction) throws IOException {
        return metrics(() -> this.delegate
                .aggregate(namespace,
                        dimension,
                        startTimestampMillis,
                        endTimestampMillis,
                        metadataQuery,
                        dimensionsQuery,
                        aggregateIntervalMillis,
                        aggregationFunction
                ), "aggregate", namespace, Map::size);
    }

    @Override
    public Set<String> metadata(final String namespace,
                                final String metadataKey,
                                final long startTimestampMillis,
                                final long endTimestampMillis,
                                final Map<String, String> metadataQuery,
                                final Map<String, String> dimensionsQuery) throws IOException {
        return metrics(() -> this.delegate
                .metadata(namespace,
                        metadataKey,
                        startTimestampMillis,
                        endTimestampMillis,
                        metadataQuery,
                        dimensionsQuery
                ), "metadata", namespace, Set::size);
    }

    @Override
    public void expire(final String namespace, final long endTimestampMillis) throws IOException {
        metrics(() -> this.delegate.expire(namespace, endTimestampMillis), "expire", namespace);
    }
}
