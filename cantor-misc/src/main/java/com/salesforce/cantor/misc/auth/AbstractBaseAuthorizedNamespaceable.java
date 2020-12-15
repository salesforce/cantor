package com.salesforce.cantor.misc.auth;

import com.salesforce.cantor.Namespaceable;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Function;

public class AbstractBaseAuthorizedNamespaceable<T extends Namespaceable> implements Namespaceable {
    private final T delegate;
    protected final Function<Request, Boolean> validRequest;

    public AbstractBaseAuthorizedNamespaceable(final T delegate,
                                               final Function<Request, Boolean> validRequest) {
        this.delegate = delegate;
        this.validRequest = validRequest;
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        return getDelegate().namespaces();
    }

    @Override
    public void create(final String namespace) throws IOException {
        final Request request = new Request(this.delegate.getClass().getName(), "create", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        getDelegate().create(namespace);
    }

    @Override
    public void drop(final String namespace) throws IOException {
        final Request request = new Request(this.delegate.getClass().getName(), "drop", namespace);
        if (!this.validRequest.apply(request)) {
            Request.throwUnauthorized(request);
        }
        getDelegate().drop(namespace);
    }

    public T getDelegate() {
        return this.delegate;
    }

    public static class Request {
        public final String methodName;
        public final String namespace;

        Request(final String name, final String methodName, final String namespace) {
            this.methodName = methodName;
            this.namespace = namespace;
        }

        public static void throwUnauthorized(final Request request) throws IOException {
            throw new IOException(String.format("User not authorized to make '%s' request on namespace '%s'", request.methodName, request.namespace));
        }
    }
}
