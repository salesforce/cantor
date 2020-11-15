/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc.open;

import com.google.protobuf.ByteString;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.grpc.objects.*;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Collection;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.grpc.open.GrpcUtils.*;

public class ObjectsGrpcService extends ObjectsServiceGrpc.ObjectsServiceImplBase {

    private final Cantor cantor;

    public ObjectsGrpcService(final Cantor cantor) {
        checkArgument(cantor != null, "null cantor");
        this.cantor = cantor;
    }

    @Override
    public void namespaces(final NamespacesRequest request, final StreamObserver<NamespacesResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            final NamespacesResponse response = NamespacesResponse.newBuilder()
                    .addAllNamespaces(getObjects().namespaces())
                    .build();
            GrpcUtils.sendResponse(responseObserver, response);
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void create(final CreateRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            getObjects().create(request.getNamespace());
            sendResponse(responseObserver, VoidResponse.getDefaultInstance());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void drop(final DropRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            getObjects().drop(request.getNamespace());
            sendResponse(responseObserver, VoidResponse.getDefaultInstance());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void keys(final KeysRequest request, final StreamObserver<KeysResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            final KeysResponse.Builder resultsBuilder = KeysResponse.newBuilder();
            final Collection<String> results = getObjects()
                    .keys(request.getNamespace(), request.getStart(), request.getCount());
            if (results != null) {
                resultsBuilder.addAllKeys(results);
            }
            sendResponse(responseObserver, resultsBuilder.build());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void get(final GetRequest request, final StreamObserver<GetResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            final GetResponse.Builder resultsBuilder = GetResponse.newBuilder();
            final byte[] value = getObjects()
                    .get(request.getNamespace(), request.getKey());
            if (value != null) {
                resultsBuilder.setValue(ByteString.copyFrom(value));
            }
            sendResponse(responseObserver, resultsBuilder.build());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void store(final StoreRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            getObjects().store(request.getNamespace(), request.getKey(), request.getValue().toByteArray());
            sendResponse(responseObserver, VoidResponse.getDefaultInstance());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void delete(final DeleteRequest request, final StreamObserver<DeleteResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            final boolean result = getObjects().delete(request.getNamespace(), request.getKey());
            sendResponse(responseObserver, DeleteResponse.newBuilder().setResult(result).build());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void size(final SizeRequest request, final StreamObserver<SizeResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            final int size = getObjects().size(request.getNamespace());
            sendResponse(responseObserver, SizeResponse.newBuilder().setSize(size).build());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    protected Objects getObjects() {
        return this.cantor.objects();
    }
}


