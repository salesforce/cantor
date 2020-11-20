/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc.auth;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.grpc.objects.*;
import com.salesforce.cantor.grpc.ObjectsGrpcService;
import io.grpc.stub.StreamObserver;

import static com.salesforce.cantor.grpc.auth.UserUtils.*;
import static com.salesforce.cantor.grpc.GrpcUtils.*;

public class AuthorizedObjectsGrpcService extends ObjectsGrpcService {

    public AuthorizedObjectsGrpcService(final Cantor cantor) {
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
    public void keys(final KeysRequest request, final StreamObserver<KeysResponse> responseObserver) {
        if (!readRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.keys(request, responseObserver);
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
    public void store(final StoreRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (!writeRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.store(request, responseObserver);
    }

    @Override
    public void delete(final DeleteRequest request, final StreamObserver<DeleteResponse> responseObserver) {
        if (!writeRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.delete(request, responseObserver);
    }

    @Override
    public void size(final SizeRequest request, final StreamObserver<SizeResponse> responseObserver) {
        if (!readRequestValid(request.getNamespace())) {
            sendUnauthorizedError(responseObserver, request.toString());
            return;
        }
        super.size(request, responseObserver);
    }
}


