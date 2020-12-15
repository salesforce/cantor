package com.salesforce.cantor.misc.auth;

import com.salesforce.cantor.*;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Function;

public class AuthorizedObjects extends AbstractBaseAuthorizedNamespaceable<Objects> implements Objects {
    public AuthorizedObjects(final Objects delegate,
                             final Function<Request, Boolean> validRequest) {
        super(delegate, validRequest);
    }

    @Override
    public void store(final String namespace, final String key, final byte[] bytes) throws IOException {
        final Request request = new Request(Objects.class.getName(), "store", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        getDelegate().store(namespace, key, bytes);
    }

    @Override
    public byte[] get(final String namespace, final String key) throws IOException {
        final Request request = new Request(Objects.class.getName(), "get", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        return getDelegate().get(namespace, key);
    }

    @Override
    public boolean delete(final String namespace, final String key) throws IOException {
        final Request request = new Request(Objects.class.getName(), "delete", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        return getDelegate().delete(namespace, key);
    }

    @Override
    public Collection<String> keys(final String namespace, final int start, final int count) throws IOException {
        final Request request = new Request(Objects.class.getName(), "keys", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        return getDelegate().keys(namespace, start, count);
    }

    @Override
    public int size(final String namespace) throws IOException {
        final Request request = new Request(Objects.class.getName(), "size", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        return getDelegate().size(namespace);
    }
}
