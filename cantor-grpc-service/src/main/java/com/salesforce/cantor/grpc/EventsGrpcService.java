/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.google.protobuf.ByteString;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.grpc.events.*;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.grpc.GrpcUtils.sendError;
import static com.salesforce.cantor.grpc.GrpcUtils.sendResponse;

public class EventsGrpcService extends EventsServiceGrpc.EventsServiceImplBase {

    private final Cantor cantor;

    public EventsGrpcService(final Cantor cantor) {
        checkArgument(cantor != null, "null cantor");
        this.cantor = cantor;
    }

    @Override
    public void namespaces(final NamespacesRequest request, final StreamObserver<NamespacesResponse> streamObserver) {
        try {
            final NamespacesResponse response = NamespacesResponse.newBuilder()
                    .addAllNamespaces(getEvents().namespaces())
                    .build();
            sendResponse(streamObserver, response);
        } catch (IOException e) {
            sendError(streamObserver, e);
        }
    }

    @Override
    public void create(final CreateRequest request, final StreamObserver<VoidResponse> streamObserver) {
        try {
            getEvents().create(request.getNamespace());
            sendResponse(streamObserver, VoidResponse.getDefaultInstance());
        } catch (IOException e) {
            sendError(streamObserver, e);
        }
    }

    @Override
    public void drop(final DropRequest request, final StreamObserver<VoidResponse> streamObserver) {
        try {
            getEvents().drop(request.getNamespace());
            sendResponse(streamObserver, VoidResponse.getDefaultInstance());
        } catch (IOException e) {
            sendError(streamObserver, e);
        }
    }

    @Override
    public void get(final GetRequest request, final StreamObserver<GetResponse> responseObserver) {
        try {
            final GetResponse.Builder responseBuilder = GetResponse.newBuilder();
            final List<Events.Event> results = getEvents().get(
                    request.getNamespace(),
                    request.getStartTimestampMillis(),
                    request.getEndTimestampMillis(),
                    request.getMetadataQueryMap(),
                    request.getDimensionsQueryMap(),
                    request.getIncludePayloads()
            );
            if (!results.isEmpty()) {
                final List<EventProto> eventProtos = new ArrayList<>(results.size());
                for (final Events.Event event : results) {
                    eventProtos.add(EventProto.newBuilder()
                            .setTimestampMillis(event.getTimestampMillis())
                            .putAllMetadata(event.getMetadata())
                            .putAllDimensions(event.getDimensions())
                            .setPayload(event.getPayload() != null ? ByteString.copyFrom(event.getPayload()) : ByteString.EMPTY)
                            .build()
                    );
                }
                responseBuilder.addAllResults(eventProtos);
            }
            sendResponse(responseObserver, responseBuilder.build());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void delete(final DeleteRequest request, final StreamObserver<DeleteResponse> responseObserver) {
        try {
            final DeleteResponse.Builder responseBuilder = DeleteResponse.newBuilder();
            final int results = getEvents().delete(
                    request.getNamespace(),
                    request.getStartTimestampMillis(),
                    request.getEndTimestampMillis(),
                    request.getMetadataQueryMap(),
                    request.getDimensionsQueryMap()
            );
            responseBuilder.setResults(results);
            sendResponse(responseObserver, responseBuilder.build());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void store(final StoreRequest request, final StreamObserver<VoidResponse> responseObserver) {
        try {
            final Collection<Events.Event> batch = new ArrayList<>();
            for (final EventProto eventProto : request.getBatchList()) {
                batch.add(new Events.Event(eventProto.getTimestampMillis(),
                        eventProto.getMetadataMap(),
                        eventProto.getDimensionsMap(),
                        eventProto.getPayload().toByteArray())
                );
            }
            getEvents().store(request.getNamespace(), batch);
            sendResponse(responseObserver, VoidResponse.getDefaultInstance());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void aggregate(final AggregateRequest request, final StreamObserver<AggregateResponse> responseObserver) {
        try {
            final Map<Long, Double> results = getEvents().aggregate(
                    request.getNamespace(),
                    request.getDimension(),
                    request.getStartTimestampMillis(),
                    request.getEndTimestampMillis(),
                    request.getMetadataQueryMap(),
                    request.getDimensionsQueryMap(),
                    request.getAggregationIntervalMillis(),
                    Events.AggregationFunction.valueOf(request.getAggregationFunction().name())
            );
            final AggregateResponse response = AggregateResponse.newBuilder().putAllResults(results).build();
            sendResponse(responseObserver, response);
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void metadata(final MetadataRequest request, final StreamObserver<MetadataResponse> responseObserver) {
        try {
            final Set<String> results = getEvents().metadata(
                    request.getNamespace(),
                    request.getMetadataKey(),
                    request.getStartTimestampMillis(),
                    request.getEndTimestampMillis(),
                    request.getMetadataQueryMap(),
                    request.getDimensionsQueryMap()
            );
            final MetadataResponse response = MetadataResponse.newBuilder().addAllValues(results).build();
            sendResponse(responseObserver, response);
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void expire(final ExpireRequest request, final StreamObserver<VoidResponse> responseObserver) {
        try {
            getEvents().expire(
                    request.getNamespace(),
                    request.getEndTimestampMillis()
            );
            sendResponse(responseObserver, VoidResponse.getDefaultInstance());
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    private Events getEvents() {
        return this.cantor.events();
    }
}


