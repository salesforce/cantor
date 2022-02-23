/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.google.protobuf.ByteString;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.grpc.events.*;
import com.salesforce.cantor.grpc.events.EventsServiceGrpc.EventsServiceBlockingStub;

import java.io.IOException;
import java.util.*;

import static com.salesforce.cantor.common.CommonUtils.nullToEmpty;
import static com.salesforce.cantor.common.EventsPreconditions.*;

public class EventsOnGrpc extends AbstractBaseGrpcClient<EventsServiceBlockingStub> implements Events {

    public EventsOnGrpc(final String target) {
        super(EventsServiceGrpc::newBlockingStub, target);
    }

    @Override
    public void create(final String namespace) throws IOException {
        checkCreate(namespace);
        call(() -> {
            final CreateRequest request = CreateRequest.newBuilder()
                    .setNamespace(namespace)
                    .build();
            getStub().create(request);
            return null;
        });
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        call(() -> {
            final DropRequest request = DropRequest.newBuilder()
                    .setNamespace(namespace)
                    .build();
            getStub().drop(request);
            return null;
        });
    }

    @Override
    public void store(final String namespace, final Collection<Event> batch) throws IOException {
        checkStore(namespace, batch);
        call(() -> {
            final List<EventProto> eventBatch = new ArrayList<>();
            for (final Event event : batch) {
                eventBatch.add(EventProto.newBuilder()
                        .setTimestampMillis(event.getTimestampMillis())
                        .putAllMetadata(event.getMetadata())
                        .putAllDimensions(event.getDimensions())
                        .setPayload(event.getPayload() != null ? ByteString.copyFrom(event.getPayload()) : ByteString.EMPTY)
                        .build()
                );
            }
            final StoreRequest request = StoreRequest.newBuilder()
                    .setNamespace(namespace)
                    .addAllBatch(eventBatch)
                    .build();
            getStub().store(request);
            return null;
        });
    }

    @Override
    public List<Event> get(final String namespace,
                           final long startTimestampMillis,
                           final long endTimestampMillis,
                           final Map<String, String> metadataQuery,
                           final Map<String, String> dimensionsQuery,
                           final boolean includePayloads,
                           final boolean ascending,
                           final int limit) throws IOException {
        checkGet(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        return call(() -> {
            final GetRequest request = GetRequest.newBuilder()
                    .setNamespace(namespace)
                    .setStartTimestampMillis(startTimestampMillis)
                    .setEndTimestampMillis(endTimestampMillis)
                    .putAllMetadataQuery(nullToEmpty(metadataQuery))
                    .putAllDimensionsQuery(nullToEmpty(dimensionsQuery))
                    .setIncludePayloads(includePayloads)
                    .setAscending(ascending)
                    .setLimit(limit)
                    .build();
            final GetResponse response = getStub().get(request);
            return getEventsFromProtos(response.getResultsList());
        });
    }

    @Override
    public Set<String> metadata(final String namespace,
                                final String metadataKey,
                                final long startTimestampMillis,
                                final long endTimestampMillis,
                                final Map<String, String> metadataQuery,
                                final Map<String, String> dimensionsQuery) throws IOException {
        checkMetadata(namespace,
                metadataKey,
                startTimestampMillis,
                endTimestampMillis,
                metadataQuery,
                dimensionsQuery
        );
        return call(() -> {
            final MetadataRequest request = MetadataRequest.newBuilder()
                    .setNamespace(namespace)
                    .setMetadataKey(metadataKey)
                    .setStartTimestampMillis(startTimestampMillis)
                    .setEndTimestampMillis(endTimestampMillis)
                    .putAllMetadataQuery(nullToEmpty(metadataQuery))
                    .putAllDimensionsQuery(nullToEmpty(dimensionsQuery))
                    .build();
            final MetadataResponse metadataResponse = getStub().metadata(request);
            return new HashSet<>(metadataResponse.getValuesList());
        });
    }

    @Override
    public List<Event> dimension(final String namespace,
                                 final String dimensionKey,
                                 final long startTimestampMillis,
                                 final long endTimestampMillis,
                                 final Map<String, String> metadataQuery,
                                 final Map<String, String> dimensionsQuery) throws IOException {
        checkDimension(namespace,
            dimensionKey,
            startTimestampMillis,
            endTimestampMillis,
            metadataQuery,
            dimensionsQuery
        );
        return call(() -> {
            final DimensionRequest request = DimensionRequest.newBuilder()
                    .setNamespace(namespace)
                    .setDimensionKey(dimensionKey)
                    .setStartTimestampMillis(startTimestampMillis)
                    .setEndTimestampMillis(endTimestampMillis)
                    .putAllMetadataQuery(nullToEmpty(metadataQuery))
                    .putAllDimensionsQuery(nullToEmpty(dimensionsQuery))
                    .build();
            final DimensionResponse dimensionResponse = getStub().dimension(request);
            return getEventsFromProtos(dimensionResponse.getValuesList());
        });
    }

    @Override
    public void expire(final String namespace, final long endTimestampMillis) throws IOException {
        checkExpire(namespace, endTimestampMillis);
        call(() -> {
            final ExpireRequest request = ExpireRequest.newBuilder()
                    .setNamespace(namespace)
                    .setEndTimestampMillis(endTimestampMillis)
                    .build();
            getStub().expire(request);
            return null;
        });
    }

    private List<Event> getEventsFromProtos(final List<EventProto> eventProtos) {
        final List<Event> events = new ArrayList<>();
        for (final EventProto proto : eventProtos) {
            final ByteString payloadByteString = proto.getPayload();
            events.add(
                    new Event(
                            proto.getTimestampMillis(),
                            proto.getMetadataMap(),
                            proto.getDimensionsMap(),
                            payloadByteString != null ? payloadByteString.toByteArray() : null
                    )
            );
        }
        return events;
    }
}

