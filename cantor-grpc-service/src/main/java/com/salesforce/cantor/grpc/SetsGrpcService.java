/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Sets;
import com.salesforce.cantor.grpc.sets.*;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.grpc.GrpcUtils.*;

public class SetsGrpcService extends SetsServiceGrpc.SetsServiceImplBase {

    private final Cantor cantor;

    public SetsGrpcService(final Cantor cantor) {
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
                    .addAllNamespaces(getSets().namespaces())
                    .build();
            sendResponse(responseObserver, response);
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
            getSets().create(request.getNamespace());
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
            getSets().drop(request.getNamespace());
            sendResponse(responseObserver, VoidResponse.getDefaultInstance());
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
            final GetResponse.Builder responseBuilder = GetResponse.newBuilder();
            final Map<String, Long> results = getSets().get(
                    request.getNamespace(),
                    request.getSet(),
                    request.getMin(),
                    request.getMax(),
                    request.getStart(),
                    request.getCount(),
                    request.getAscending()
            );
            if (!results.isEmpty()) {
                responseBuilder.putAllEntries(results);
            }
            sendResponse(responseObserver, responseBuilder.build());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void union(final UnionRequest request, final StreamObserver<UnionResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            final UnionResponse.Builder responseBuilder = UnionResponse.newBuilder();
            final Map<String, Long> results = getSets().union(
                    request.getNamespace(),
                    request.getSetsList(),
                    request.getMin(),
                    request.getMax(),
                    request.getStart(),
                    request.getCount(),
                    request.getAscending()
            );
            if (!results.isEmpty()) {
                responseBuilder.putAllEntries(results);
            }
            sendResponse(responseObserver, responseBuilder.build());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void intersect(final IntersectRequest request, final StreamObserver<IntersectResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            final IntersectResponse.Builder responseBuilder = IntersectResponse.newBuilder();
            final Map<String, Long> results = getSets().intersect(
                    request.getNamespace(),
                    request.getSetsList(),
                    request.getMin(),
                    request.getMax(),
                    request.getStart(),
                    request.getCount(),
                    request.getAscending()
            );
            if (!results.isEmpty()) {
                responseBuilder.putAllEntries(results);
            }
            sendResponse(responseObserver, responseBuilder.build());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void pop(final PopRequest request, final StreamObserver<PopResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            final PopResponse.Builder responseBuilder = PopResponse.newBuilder();
            final Map<String, Long> results = getSets().pop(
                    request.getNamespace(),
                    request.getSet(),
                    request.getMin(),
                    request.getMax(),
                    request.getStart(),
                    request.getCount(),
                    request.getAscending()
            );
            if (!results.isEmpty()) {
                responseBuilder.putAllEntries(results);
            }
            sendResponse(responseObserver, responseBuilder.build());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void add(final AddRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            getSets()
                    .add(request.getNamespace(), request.getSet(), request.getEntry(), request.getWeight());
            sendResponse(responseObserver, VoidResponse.getDefaultInstance());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void addBatch(final AddBatchRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            getSets().add(request.getNamespace(), request.getSet(), request.getEntriesMap());
            sendResponse(responseObserver, VoidResponse.getDefaultInstance());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void deleteBetween(final DeleteBetweenRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            getSets()
                    .delete(request.getNamespace(), request.getSet(), request.getMin(), request.getMax());
            sendResponse(responseObserver, VoidResponse.getDefaultInstance());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void deleteEntry(final DeleteEntryRequest request, final StreamObserver<DeleteEntryResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            final boolean deleted = getSets()
                    .delete(request.getNamespace(), request.getSet(), request.getEntry());
            sendResponse(responseObserver, DeleteEntryResponse.newBuilder().setDeleted(deleted).build());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void deleteBatch(final DeleteBatchRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            getSets().delete(request.getNamespace(), request.getSet(), request.getEntriesList());
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
            final KeysResponse.Builder responseBuilder = KeysResponse.newBuilder();
            final Collection<String> keys = getSets().entries(
                    request.getNamespace(),
                    request.getSet(),
                    request.getMin(),
                    request.getMax(),
                    request.getStart(),
                    request.getCount(),
                    request.getAscending()
            );
            if (!keys.isEmpty()) {
                responseBuilder.addAllKeys(keys);
            }
            sendResponse(responseObserver, responseBuilder.build());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void sets(final SetsRequest request, final StreamObserver<SetsResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            final SetsResponse.Builder responseBuilder = SetsResponse.newBuilder();
            final Collection<String> sets = getSets().sets(request.getNamespace());
            if (!sets.isEmpty()) {
                responseBuilder.addAllSets(sets);
            }
            sendResponse(responseObserver, responseBuilder.build());
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
            final int size = getSets().size(request.getNamespace(), request.getSet());
            sendResponse(responseObserver, SizeResponse.newBuilder().setSize(size).build());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void weight(final WeightRequest request, final StreamObserver<WeightResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            final Long weight = getSets().weight(request.getNamespace(), request.getSet(), request.getEntry());
            final WeightResponse.Builder responseBuilder = WeightResponse.newBuilder();
            if (weight == null) {
                responseBuilder.setFound(false);
            } else {
                responseBuilder.setFound(true);
                responseBuilder.setWeight(weight);
            }
            sendResponse(responseObserver, responseBuilder.build());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void inc(final IncRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            getSets().inc(request.getNamespace(), request.getSet(), request.getEntry(), request.getCount());
            sendResponse(responseObserver, VoidResponse.getDefaultInstance());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    private Sets getSets() {
        return this.cantor.sets();
    }
}


