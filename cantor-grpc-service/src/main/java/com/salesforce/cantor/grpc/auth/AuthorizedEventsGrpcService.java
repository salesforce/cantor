/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc.auth;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.grpc.events.*;
import com.salesforce.cantor.grpc.EventsGrpcService;
import io.grpc.stub.StreamObserver;

import static com.salesforce.cantor.grpc.auth.UserUtils.*;
import static com.salesforce.cantor.grpc.GrpcUtils.*;


public class AuthorizedEventsGrpcService extends EventsGrpcService {

    public AuthorizedEventsGrpcService(final Cantor cantor) {
        super(cantor);
    }

    @Override
    public void namespaces(final NamespacesRequest request, final StreamObserver<NamespacesResponse> responseObserver) {
        super.namespaces(request, responseObserver);
    }

    @Override
    public void create(final CreateRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (!writeRequestValid(request.getNamespace())) {
            sendError(responseObserver, new UnauthorizedException(request.toString()));
            return;
        }
        super.create(request, responseObserver);
    }

    @Override
    public void drop(final DropRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (!writeRequestValid(request.getNamespace())) {
            sendError(responseObserver, new UnauthorizedException(request.toString()));
            return;
        }
        super.drop(request, responseObserver);
    }

    @Override
    public void get(final GetRequest request, final StreamObserver<GetResponse> responseObserver) {
        if (!readRequestValid(request.getNamespace())) {
            sendError(responseObserver, new UnauthorizedException(request.toString()));
            return;
        }
        super.get(request, responseObserver);
    }

    @Override
    public void delete(final DeleteRequest request, final StreamObserver<DeleteResponse> responseObserver) {
        if (!writeRequestValid(request.getNamespace())) {
            sendError(responseObserver, new UnauthorizedException(request.toString()));
            return;
        }
        super.delete(request, responseObserver);
    }

    @Override
    public void store(final StoreRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (!writeRequestValid(request.getNamespace())) {
            sendError(responseObserver, new UnauthorizedException(request.toString()));
            return;
        }
        super.store(request, responseObserver);
    }

    @Override
    public void aggregate(final AggregateRequest request, final StreamObserver<AggregateResponse> responseObserver) {
        if (!readRequestValid(request.getNamespace())) {
            sendError(responseObserver, new UnauthorizedException(request.toString()));
            return;
        }
        super.aggregate(request, responseObserver);
    }

    @Override
    public void metadata(final MetadataRequest request, final StreamObserver<MetadataResponse> responseObserver) {
        if (!readRequestValid(request.getNamespace())) {
            sendError(responseObserver, new UnauthorizedException(request.toString()));
            return;
        }
        super.metadata(request, responseObserver);
    }

    @Override
    public void expire(final ExpireRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (!writeRequestValid(request.getNamespace())) {
            sendError(responseObserver, new UnauthorizedException(request.toString()));
            return;
        }
        super.expire(request, responseObserver);
    }
}


