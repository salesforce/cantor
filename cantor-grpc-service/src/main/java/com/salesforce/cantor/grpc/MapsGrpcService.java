/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Maps;
import com.salesforce.cantor.grpc.maps.*;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.grpc.GrpcUtils.*;

public class MapsGrpcService extends MapsServiceGrpc.MapsServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(MapsGrpcService.class);
    private final Cantor cantor;

    public MapsGrpcService(final Cantor cantor) {
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
                    .addAllNamespaces(getMaps().namespaces())
                    .build();
            GrpcUtils.sendResponse(responseObserver, response);
        } catch (IOException e) {
            logger.warn("exception caught handling namespaces: {}", e.getMessage());
            logger.debug("exception caught handling namespaces: ", e);
            GrpcUtils.sendError(responseObserver, e);
        }
    }

    @Override
    public void create(final CreateRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            getMaps().create(request.getNamespace());
            sendResponse(responseObserver, VoidResponse.getDefaultInstance());
        } catch (IOException e) {
            logger.warn("exception caught handling get range: {}", e.getMessage());
            logger.debug("exception caught handling get range: ", e);
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
            getMaps().drop(request.getNamespace());
            sendResponse(responseObserver, VoidResponse.getDefaultInstance());
        } catch (IOException e) {
            logger.warn("exception caught handling get range: {}", e.getMessage());
            logger.debug("exception caught handling get range: ", e);
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
            getMaps().store(request.getNamespace(), request.getMapMap());
            sendResponse(responseObserver, VoidResponse.getDefaultInstance());
        } catch (IOException e) {
            logger.warn("exception caught handling get: {}", e.getMessage());
            logger.debug("exception caught handling get: ", e);
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
            final Collection<Map<String, String>> results = getMaps().get(request.getNamespace(), request.getQueryMap());
            for (final Map<String, String> map : results) {
                resultsBuilder.addResults(MapProto.newBuilder().putAllMap(map));
            }
            sendResponse(responseObserver, resultsBuilder.build());
        } catch (IOException e) {
            logger.warn("exception caught handling get: {}", e.getMessage());
            logger.debug("exception caught handling get: ", e);
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
            final int results = getMaps().delete(request.getNamespace(), request.getQueryMap());
            sendResponse(responseObserver, DeleteResponse.newBuilder().setResults(results).build());
        } catch (IOException e) {
            logger.warn("exception caught handling get: {}", e.getMessage());
            logger.debug("exception caught handling get: ", e);
            sendError(responseObserver, e);
        }
    }

    protected Maps getMaps() {
        return this.cantor.maps();
    }
}


