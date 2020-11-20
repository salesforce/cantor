/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc.auth;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.grpc.SetsGrpcService;
import com.salesforce.cantor.grpc.sets.*;
import io.grpc.stub.StreamObserver;

import static com.salesforce.cantor.grpc.auth.UserUtils.*;
import static com.salesforce.cantor.grpc.GrpcUtils.*;

public class AuthorizedSetsGrpcService extends SetsGrpcService {

    public AuthorizedSetsGrpcService(final Cantor cantor) {
        super(cantor);
    }

    @Override
    public void namespaces(final NamespacesRequest request, final StreamObserver<NamespacesResponse> responseObserver) {
        super.namespaces(request, responseObserver);
    }

    @Override
    public void create(final CreateRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (!writeRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.create(request, responseObserver);
    }

    @Override
    public void drop(final DropRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (!writeRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.drop(request, responseObserver);
    }

    @Override
    public void get(final GetRequest request, final StreamObserver<GetResponse> responseObserver) {
        if (!readRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.get(request, responseObserver);
    }

    @Override
    public void union(final UnionRequest request, final StreamObserver<UnionResponse> responseObserver) {
        if (!readRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.union(request, responseObserver);
    }

    @Override
    public void intersect(final IntersectRequest request, final StreamObserver<IntersectResponse> responseObserver) {
        if (!readRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.intersect(request, responseObserver);
    }

    @Override
    public void pop(final PopRequest request, final StreamObserver<PopResponse> responseObserver) {
        if (!writeRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.pop(request, responseObserver);
    }

    @Override
    public void add(final AddRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (!writeRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.add(request, responseObserver);
    }

    @Override
    public void addBatch(final AddBatchRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (!writeRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.addBatch(request, responseObserver);
    }

    @Override
    public void deleteBetween(final DeleteBetweenRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (!writeRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.deleteBetween(request, responseObserver);
    }

    @Override
    public void deleteEntry(final DeleteEntryRequest request, final StreamObserver<DeleteEntryResponse> responseObserver) {
        if (!writeRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.deleteEntry(request, responseObserver);
    }

    @Override
    public void deleteBatch(final DeleteBatchRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (!writeRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.deleteBatch(request, responseObserver);
    }

    @Override
    public void keys(final KeysRequest request, final StreamObserver<KeysResponse> responseObserver) {
        if (!readRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.keys(request, responseObserver);
    }

    @Override
    public void sets(final SetsRequest request, final StreamObserver<SetsResponse> responseObserver) {
        if (!readRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.sets(request, responseObserver);
    }

    @Override
    public void size(final SizeRequest request, final StreamObserver<SizeResponse> responseObserver) {
        if (!readRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.size(request, responseObserver);
    }

    @Override
    public void weight(final WeightRequest request, final StreamObserver<WeightResponse> responseObserver) {
        if (!readRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.weight(request, responseObserver);
    }

    @Override
    public void inc(final IncRequest request, final StreamObserver<IncResponse> responseObserver) {
        if (!writeRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.inc(request, responseObserver);
    }
}


