/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.google.protobuf.ByteString;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.grpc.objects.*;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.grpc.GrpcUtils.sendError;
import static com.salesforce.cantor.grpc.GrpcUtils.sendResponse;

public class ObjectsGrpcService extends ObjectsServiceGrpc.ObjectsServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(ObjectsGrpcService.class);
    private final Cantor cantor;

    public ObjectsGrpcService(final Cantor cantor) {
        checkArgument(cantor != null, "null cantor");
        this.cantor = cantor;
    }

    @Override
    public void namespaces(final NamespacesRequest request, final StreamObserver<NamespacesResponse> streamObserver) {
        try {
            final NamespacesResponse response = NamespacesResponse.newBuilder()
                    .addAllNamespaces(getObjects().namespaces())
                    .build();
            GrpcUtils.sendResponse(streamObserver, response);
        } catch (IOException e) {
            sendError(streamObserver, e);
        }
    }

    @Override
    public void create(final CreateRequest request, final StreamObserver<VoidResponse> streamObserver) {
        try {
            getObjects().create(request.getNamespace());
            sendResponse(streamObserver, VoidResponse.getDefaultInstance());
        } catch (IOException e) {
            sendError(streamObserver, e);
        }
    }

    @Override
    public void drop(final DropRequest request, final StreamObserver<VoidResponse> streamObserver) {
        try {
            getObjects().drop(request.getNamespace());
            sendResponse(streamObserver, VoidResponse.getDefaultInstance());
        } catch (IOException e) {
            sendError(streamObserver, e);
        }
    }

    @Override
    public void keys(final KeysRequest request, final StreamObserver<KeysResponse> streamObserver) {
        try {
            final KeysResponse.Builder resultsBuilder = KeysResponse.newBuilder();
            final Collection<String> results = getObjects()
                    .keys(request.getNamespace(), request.getStart(), request.getCount());
            if (results != null) {
                resultsBuilder.addAllKeys(results);
            }
            sendResponse(streamObserver, resultsBuilder.build());
        } catch (IOException e) {
            sendError(streamObserver, e);
        }
    }

    @Override
    public void get(final GetRequest request, final StreamObserver<GetResponse> streamObserver) {
        try {
            final GetResponse.Builder resultsBuilder = GetResponse.newBuilder();
            final byte[] value = getObjects()
                    .get(request.getNamespace(), request.getKey());
            if (value != null) {
                resultsBuilder.setValue(ByteString.copyFrom(value));
            }
            sendResponse(streamObserver, resultsBuilder.build());
        } catch (IOException e) {
            sendError(streamObserver, e);
        }
    }

    @Override
    public void store(final StoreRequest request, final StreamObserver<VoidResponse> streamObserver) {
        try {
            getObjects().store(request.getNamespace(), request.getKey(), request.getValue().toByteArray());
            sendResponse(streamObserver, VoidResponse.getDefaultInstance());
        } catch (IOException e) {
            sendError(streamObserver, e);
        }
    }

    @Override
    public void delete(final DeleteRequest request, final StreamObserver<DeleteResponse> streamObserver) {
        try {
            final boolean result = getObjects().delete(request.getNamespace(), request.getKey());
            sendResponse(streamObserver, DeleteResponse.newBuilder().setResult(result).build());
        } catch (IOException e) {
            sendError(streamObserver, e);
        }
    }

    @Override
    public void size(final SizeRequest request, final StreamObserver<SizeResponse> streamObserver) {
        try {
            final int size = getObjects().size(request.getNamespace());
            sendResponse(streamObserver, SizeResponse.newBuilder().setSize(size).build());
        } catch (IOException e) {
            sendError(streamObserver, e);
        }
    }

    protected Objects getObjects() {
        return this.cantor.objects();
    }
}


