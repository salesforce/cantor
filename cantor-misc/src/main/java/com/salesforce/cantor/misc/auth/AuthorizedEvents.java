package com.salesforce.cantor.misc.auth;

import com.salesforce.cantor.Events;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

public class AuthorizedEvents extends AbstractBaseAuthorizedNamespaceable<Events> implements Events {
    public AuthorizedEvents(final Events delegate,
                            final Function<Request, Boolean> validRequest) {
        super(delegate, validRequest);
    }

    @Override
    public void store(final String namespace, final Collection<Event> batch) throws IOException {
        final Request request = new Request(Events.class.getName(), "store", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        getDelegate().store(namespace, batch);
    }

    @Override
    public List<Event> get(final String namespace, final long startTimestampMillis, final long endTimestampMillis, final Map<String, String> metadataQuery, final Map<String, String> dimensionsQuery, final boolean includePayloads, final boolean ascending, final int limit) throws IOException {
        final Request request = new Request(Events.class.getName(), "get", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        return getDelegate().get(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery, includePayloads, ascending, limit);
    }

    @Override
    public int delete(final String namespace, final long startTimestampMillis, final long endTimestampMillis, final Map<String, String> metadataQuery, final Map<String, String> dimensionsQuery) throws IOException {
        final Request request = new Request(Events.class.getName(), "delete", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        return getDelegate().delete(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
    }

    @Override
    public Map<Long, Double> aggregate(final String namespace, final String dimension, final long startTimestampMillis, final long endTimestampMillis, final Map<String, String> metadataQuery, final Map<String, String> dimensionsQuery, final int aggregateIntervalMillis, final AggregationFunction aggregationFunction) throws IOException {
        final Request request = new Request(Events.class.getName(), "aggregate", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        return getDelegate().aggregate(namespace, dimension, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery, aggregateIntervalMillis, aggregationFunction);
    }

    @Override
    public Set<String> metadata(final String namespace, final String metadataKey, final long startTimestampMillis, final long endTimestampMillis, final Map<String, String> metadataQuery, final Map<String, String> dimensionsQuery) throws IOException {
        final Request request = new Request(Events.class.getName(), "metadata", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        return getDelegate().metadata(namespace, metadataKey, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
    }

    @Override
    public void expire(final String namespace, final long endTimestampMillis) throws IOException {
        final Request request = new Request(Events.class.getName(), "expire", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        getDelegate().expire(namespace, endTimestampMillis);
    }
}
