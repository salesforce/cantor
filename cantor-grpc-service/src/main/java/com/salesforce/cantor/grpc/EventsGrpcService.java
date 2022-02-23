/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.google.protobuf.ByteString;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.grpc.events.*;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.*;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.grpc.GrpcUtils.*;

public class EventsGrpcService extends EventsServiceGrpc.EventsServiceImplBase {

    private final Cantor cantor;

    public EventsGrpcService(final Cantor cantor) {
        checkArgument(cantor != null, "null cantor");
        this.cantor = cantor;
    }

    @Override
    public void create(final CreateRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            getEvents().create(request.getNamespace());
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
            getEvents().drop(request.getNamespace());
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
            final List<Events.Event> results = getEvents().get(
                    request.getNamespace(),
                    request.getStartTimestampMillis(),
                    request.getEndTimestampMillis(),
                    request.getMetadataQueryMap(),
                    request.getDimensionsQueryMap(),
                    request.getIncludePayloads(),
                    request.getAscending(),
                    request.getLimit());
            if (!results.isEmpty()) {
                responseBuilder.addAllResults(getProtosFromEvents(results));
            }
            sendResponse(responseObserver, responseBuilder.build());
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
    public void metadata(final MetadataRequest request, final StreamObserver<MetadataResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
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
    public void dimension(final DimensionRequest request, final StreamObserver<DimensionResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
        try {
            final List<Events.Event> results = getEvents().dimension(
                    request.getNamespace(),
                    request.getDimensionKey(),
                    request.getStartTimestampMillis(),
                    request.getEndTimestampMillis(),
                    request.getMetadataQueryMap(),
                    request.getDimensionsQueryMap()
            );
            final DimensionResponse response = DimensionResponse.newBuilder().addAllValues(getProtosFromEvents(results)).build();
            sendResponse(responseObserver, response);
        } catch (IOException e) {
            sendError(responseObserver, e);
        }
    }

    @Override
    public void expire(final ExpireRequest request, final StreamObserver<VoidResponse> responseObserver) {
        if (Context.current().isCancelled()) {
            sendCancelledError(responseObserver, Context.current().cancellationCause());
            return;
        }
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

    private List<EventProto> getProtosFromEvents(final List<Events.Event> events) {
        final List<EventProto> eventProtos = new ArrayList<>();
        for (final Events.Event event : events) {
            eventProtos.add(EventProto.newBuilder()
                    .setTimestampMillis(event.getTimestampMillis())
                    .putAllMetadata(event.getMetadata())
                    .putAllDimensions(event.getDimensions())
                    .setPayload(event.getPayload() != null ? ByteString.copyFrom(event.getPayload()) : ByteString.EMPTY)
                    .build()
            );
        }
        return eventProtos;
    }
}


