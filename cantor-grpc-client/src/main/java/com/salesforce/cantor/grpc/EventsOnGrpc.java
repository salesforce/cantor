/*
 * Copyright (c) 2019, Salesforce.com, Inc.
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

    public EventsOnGrpc(final String target, final long timeoutMillis) {
        super(EventsServiceGrpc::newBlockingStub, target, timeoutMillis);
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        return call(() -> {
            final NamespacesRequest request = NamespacesRequest.newBuilder().build();
            return getStub().namespaces(request).getNamespacesList();
        });
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
                           final boolean includePayloads) throws IOException {
        checkGet(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        return call(() -> {
            final GetRequest request = GetRequest.newBuilder()
                    .setNamespace(namespace)
                    .setStartTimestampMillis(startTimestampMillis)
                    .setEndTimestampMillis(endTimestampMillis)
                    .putAllMetadataQuery(nullToEmpty(metadataQuery))
                    .putAllDimensionsQuery(nullToEmpty(dimensionsQuery))
                    .setIncludePayloads(includePayloads)
                    .build();
            final List<Event> results = new ArrayList<>();
            final GetResponse response = getStub().get(request);
            final List<EventProto> eventProtos = response.getResultsList();
            for (final EventProto proto : eventProtos) {
                final ByteString payloadByteString = proto.getPayload();
                results.add(
                        new Event(
                                proto.getTimestampMillis(),
                                proto.getMetadataMap(),
                                proto.getDimensionsMap(),
                                payloadByteString != null ? payloadByteString.toByteArray() : null
                        )
                );
            }
            // sort all results
            results.sort((event1, event2) -> {
                if (event1.getTimestampMillis() < event2.getTimestampMillis()) {
                    return -1;
                } else if (event1.getTimestampMillis() > event2.getTimestampMillis()) {
                    return 1;
                }
                return 0;
            });
            return results;
        });
    }

    @Override
    public int delete(final String namespace,
                      final long startTimestampMillis,
                      final long endTimestampMillis,
                      final Map<String, String> metadataQuery,
                      final Map<String, String> dimensionsQuery) throws IOException {
        checkDelete(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        return call(() -> {
            final DeleteRequest request = DeleteRequest.newBuilder()
                    .setNamespace(namespace)
                    .setStartTimestampMillis(startTimestampMillis)
                    .setEndTimestampMillis(endTimestampMillis)
                    .putAllMetadataQuery(nullToEmpty(metadataQuery))
                    .putAllDimensionsQuery(nullToEmpty(dimensionsQuery))
                    .build();
            final DeleteResponse response = getStub().delete(request);
            return response.getResults();
        });
    }

    @Override
    public Map<Long, Double> aggregate(final String namespace,
                                       final String dimension,
                                       final long startTimestampMillis,
                                       final long endTimestampMillis,
                                       final Map<String, String> metadataQuery,
                                       final Map<String, String> dimensionsQuery,
                                       final int aggregateIntervalMillis,
                                       final AggregationFunction aggregationFunction) throws IOException {
        checkAggregate(namespace,
                dimension,
                startTimestampMillis,
                endTimestampMillis,
                metadataQuery,
                dimensionsQuery,
                aggregateIntervalMillis,
                aggregationFunction
        );
        final Map<Long, Double> results = new HashMap<>();
        return call(() -> {
            final AggregateRequest request = AggregateRequest.newBuilder()
                    .setNamespace(namespace)
                    .setDimension(dimension)
                    .setStartTimestampMillis(startTimestampMillis)
                    .setEndTimestampMillis(endTimestampMillis)
                    .putAllMetadataQuery(nullToEmpty(metadataQuery))
                    .putAllDimensionsQuery(nullToEmpty(dimensionsQuery))
                    .setAggregationIntervalMillis(aggregateIntervalMillis)
                    .setAggregationFunction(AggregateRequest.AggregationFunction.valueOf(aggregationFunction.name()))
                    .build();
            final AggregateResponse aggregateResponse = getStub().aggregate(request);
            for (final Map.Entry<Long, Double> entry : aggregateResponse.getResultsMap().entrySet()) {
                final long timestamp = entry.getKey();
                final double value = entry.getValue();
                if (!results.containsKey(entry.getKey())) {
                    results.put(timestamp, value);
                } else {
                    switch (aggregationFunction) {
                        case AVG:
                            results.put(timestamp, results.get(timestamp) + value / 2.0);
                            break;
                        case MAX:
                            results.put(timestamp, Math.max(results.get(timestamp), value));
                            break;
                        case MIN:
                            results.put(timestamp, Math.min(results.get(timestamp), value));
                            break;
                        case SUM:
                            results.put(timestamp, results.get(timestamp) + value);
                            break;
                        default:
                            throw new IllegalStateException();
                    }
                }
            }
            return results;
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
}

