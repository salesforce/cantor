/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.google.protobuf.Message;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.grpc.events.NamespacesRequest;
import com.salesforce.cantor.grpc.objects.CreateRequest;
import com.salesforce.cantor.grpc.objects.NamespacesResponse;
import com.salesforce.cantor.grpc.objects.VoidResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ProtobufRequestHandlers {
    private final Map<Class<? extends Message>, RequestHandler> handlers = new HashMap<>();
    private final Cantor delegate;

    public ProtobufRequestHandlers(final Cantor delegate) {
        this.delegate = delegate;
        this.handlers.put(NamespacesRequest.class, new EventsRequestHandlers.NamespaceRequestHandler());
        this.handlers.put(CreateRequest.class, new EventsRequestHandlers.CreateRequestHandler());
    }

    public Message handle(final Message request) throws IOException {
        return this.handlers.get(request.getClass()).handle(getDelegate(), request);
    }

    private Cantor getDelegate() {
        return this.delegate;
    }

    private interface RequestHandler {
        Message handle(Cantor delegate, Message message) throws IOException;
    }

    private static class ObjectsRequestHandlers {
        private static class NamespaceRequestHandler implements RequestHandler {
            @Override
            public NamespacesResponse handle(final Cantor delegate, final Message ignored) throws IOException {
                final NamespacesResponse response = NamespacesResponse.newBuilder()
                        .addAllNamespaces(delegate.events().namespaces())
                        .build();
                return response;
            }
        }

        private static class CreateRequestHandler implements RequestHandler {
            @Override
            public VoidResponse handle(final Cantor delegate, final Message message) throws IOException {
                final CreateRequest request = (CreateRequest) message;
                delegate.events().create(request.getNamespace());
                return VoidResponse.getDefaultInstance();
            }
        }
    }


    private static class SetsRequestHandlers {
        private static class NamespaceRequestHandler implements RequestHandler {
            @Override
            public NamespacesResponse handle(final Cantor delegate, final Message ignored) throws IOException {
                final NamespacesResponse response = NamespacesResponse.newBuilder()
                        .addAllNamespaces(delegate.events().namespaces())
                        .build();
                return response;
            }
        }

        private static class CreateRequestHandler implements RequestHandler {
            @Override
            public VoidResponse handle(final Cantor delegate, final Message message) throws IOException {
                final CreateRequest request = (CreateRequest) message;
                delegate.events().create(request.getNamespace());
                return VoidResponse.getDefaultInstance();
            }
        }
    }

    private static class EventsRequestHandlers {
        private static class NamespaceRequestHandler implements RequestHandler {
            @Override
            public NamespacesResponse handle(final Cantor delegate, final Message ignored) throws IOException {
                final NamespacesResponse response = NamespacesResponse.newBuilder()
                        .addAllNamespaces(delegate.events().namespaces())
                        .build();
                return response;
            }
        }

        private static class CreateRequestHandler implements RequestHandler {
            @Override
            public VoidResponse handle(final Cantor delegate, final Message message) throws IOException {
                final CreateRequest request = (CreateRequest) message;
                delegate.events().create(request.getNamespace());
                return VoidResponse.getDefaultInstance();
            }
        }
    }


}
