package com.salesforce.cantor.misc.auth;

import com.salesforce.cantor.Sets;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

public class AuthorizedSets extends AbstractBaseAuthorizedNamespaceable<Sets> implements Sets {
    public AuthorizedSets(final Sets delegate,
                          final Function<Request, Boolean> validRequest) {
        super(delegate, validRequest);
    }

    @Override
    public void add(final String namespace, final String set, final String entry, final long weight) throws IOException {
        final Request request = new Request(Sets.class.getName(), "add", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        getDelegate().add(namespace, set, entry, weight);
    }

    @Override
    public void add(final String namespace, final String set, final Map<String, Long> entries) throws IOException {
        final Request request = new Request(Sets.class.getName(), "add", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        getDelegate().add(namespace, set, entries);
    }

    @Override
    public Collection<String> entries(final String namespace, final String set, final long min, final long max, final int start, final int count, final boolean ascending) throws IOException {
        final Request request = new Request(Sets.class.getName(), "entries", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        return getDelegate().entries(namespace, set, min, max, start, count, ascending);
    }

    @Override
    public Map<String, Long> get(final String namespace, final String set, final long min, final long max, final int start, final int count, final boolean ascending) throws IOException {
        final Request request = new Request(Sets.class.getName(), "get", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        return getDelegate().get(namespace, set, min, max, start, count, ascending);
    }

    @Override
    public void delete(final String namespace, final String set, final long min, final long max) throws IOException {
        final Request request = new Request(Sets.class.getName(), "drop", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        getDelegate().delete(namespace, set, min, max);
    }

    @Override
    public boolean delete(final String namespace, final String set, final String entry) throws IOException {
        final Request request = new Request(Sets.class.getName(), "delete", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        return getDelegate().delete(namespace, set, entry);
    }

    @Override
    public void delete(final String namespace, final String set, final Collection<String> entries) throws IOException {
        final Request request = new Request(Sets.class.getName(), "delete", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        getDelegate().delete(namespace, set, entries);
    }

    @Override
    public Map<String, Long> union(final String namespace, final Collection<String> sets, final long min, final long max, final int start, final int count, final boolean ascending) throws IOException {
        final Request request = new Request(Sets.class.getName(), "union", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        return getDelegate().union(namespace, sets, min, max, start, count, ascending);
    }

    @Override
    public Map<String, Long> intersect(final String namespace, final Collection<String> sets, final long min, final long max, final int start, final int count, final boolean ascending) throws IOException {
        final Request request = new Request(Sets.class.getName(), "intersect", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        return getDelegate().intersect(namespace, sets, min, max, start, count, ascending);
    }

    @Override
    public Map<String, Long> pop(final String namespace, final String set, final long min, final long max, final int start, final int count, final boolean ascending) throws IOException {
        final Request request = new Request(Sets.class.getName(), "pop", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        return getDelegate().pop(namespace, set, min, max, start, count, ascending);
    }

    @Override
    public Collection<String> sets(final String namespace) throws IOException {
        final Request request = new Request(Sets.class.getName(), "sets", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        return getDelegate().sets(namespace);
    }

    @Override
    public int size(final String namespace, final String set) throws IOException {
        final Request request = new Request(Sets.class.getName(), "size", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        return getDelegate().size(namespace, set);
    }

    @Override
    public Long weight(final String namespace, final String set, final String entry) throws IOException {
        final Request request = new Request(Sets.class.getName(), "weight", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        return getDelegate().weight(namespace, set, entry);
    }

    @Override
    public long inc(final String namespace, final String set, final String entry, final long count) throws IOException {
        final Request request = new Request(Sets.class.getName(), "inc", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        return getDelegate().inc(namespace, set, entry, count);
    }
}
