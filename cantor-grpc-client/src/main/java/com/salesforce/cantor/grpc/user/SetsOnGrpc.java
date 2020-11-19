/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc.user;

import com.salesforce.cantor.Sets;
import com.salesforce.cantor.management.CantorCredentials;
import com.salesforce.cantor.grpc.sets.*;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.salesforce.cantor.common.SetsPreconditions.*;
import static com.salesforce.cantor.grpc.sets.SetsServiceGrpc.SetsServiceBlockingStub;

public class SetsOnGrpc extends AbstractBaseGrpcClient<SetsServiceBlockingStub> implements Sets {

    public SetsOnGrpc(final String target) {
        super(SetsServiceGrpc::newBlockingStub, target);
    }

    public SetsOnGrpc(final String target, final CantorCredentials credentials) {
        super(SetsServiceGrpc::newBlockingStub, target, credentials);
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
    public Map<String, Long> get(final String namespace,
                                 final String set,
                                 final long min,
                                 final long max,
                                 final int start,
                                 final int count,
                                 final boolean ascending) throws IOException {
        checkGet(namespace, set, min, max, start, count, ascending);
        return call(() -> {
            final GetRequest request = GetRequest.newBuilder()
                    .setNamespace(namespace)
                    .setSet(set)
                    .setMin(min)
                    .setMax(max)
                    .setStart(start)
                    .setCount(count)
                    .setAscending(ascending)
                    .build();
            return getStub().get(request).getEntriesMap();
        });
    }

    @Override
    public Map<String, Long> union(final String namespace,
                                   final Collection<String> sets,
                                   final long min,
                                   final long max,
                                   final int start,
                                   final int count,
                                   final boolean ascending) throws IOException {
        checkUnion(namespace, sets, min, max, start, count, ascending);
        return call(() -> {
            final UnionRequest request = UnionRequest.newBuilder()
                    .setNamespace(namespace)
                    .addAllSets(sets)
                    .setMin(min)
                    .setMax(max)
                    .setStart(start)
                    .setCount(count)
                    .setAscending(ascending)
                    .build();
            return getStub().union(request).getEntriesMap();
        });
    }

    @Override
    public Map<String, Long> intersect(final String namespace,
                                       final Collection<String> sets,
                                       final long min,
                                       final long max,
                                       final int start,
                                       final int count,
                                       final boolean ascending) throws IOException {
        checkIntersect(namespace, sets, min, max, start, count, ascending);
        return call(() -> {
            final IntersectRequest request = IntersectRequest.newBuilder()
                    .setNamespace(namespace)
                    .addAllSets(sets)
                    .setMin(min)
                    .setMax(max)
                    .setStart(start)
                    .setCount(count)
                    .setAscending(ascending)
                    .build();
            return getStub().intersect(request).getEntriesMap();
        });
    }

    @Override
    public Map<String, Long> pop(final String namespace,
                                 final String set,
                                 final long min,
                                 final long max,
                                 final int start,
                                 final int count,
                                 final boolean ascending) throws IOException {
        checkPop(namespace, set, min, max, start, count, ascending);
        return call(() -> {
            final PopRequest request = PopRequest.newBuilder()
                    .setNamespace(namespace)
                    .setSet(set)
                    .setMin(min)
                    .setMax(max)
                    .setStart(start)
                    .setCount(count)
                    .setAscending(ascending)
                    .build();
            return getStub().pop(request).getEntriesMap();
        });
    }

    @Override
    public void add(final String namespace, final String set, final String entry, final long weight) throws IOException {
        checkAdd(namespace, set, entry, weight);
        call(() -> {
            final AddRequest request = AddRequest.newBuilder()
                    .setNamespace(namespace)
                    .setSet(set)
                    .setEntry(entry)
                    .setWeight(weight)
                    .build();
            getStub().add(request);
            return null;
        });
    }

    @Override
    public void add(final String namespace, final String set, final Map<String, Long> entries) throws IOException {
        checkAdd(namespace, set, entries);
        call(() -> {
            final AddBatchRequest request = AddBatchRequest.newBuilder()
                    .setNamespace(namespace)
                    .setSet(set)
                    .putAllEntries(entries)
                    .build();
            getStub().addBatch(request);
            return null;
        });
    }

    @Override
    public void delete(final String namespace, final String set, final long min, final long max) throws IOException {
        checkDelete(namespace, set, min, max);
        call(() -> {
            final DeleteBetweenRequest request = DeleteBetweenRequest.newBuilder()
                    .setNamespace(namespace)
                    .setSet(set)
                    .setMin(min)
                    .setMax(max)
                    .build();
            getStub().deleteBetween(request);
            return null;
        });
    }

    @Override
    public boolean delete(final String namespace, final String set, final String entry) throws IOException {
        checkDelete(namespace, set, entry);
        return call(() -> {
            final DeleteEntryRequest request = DeleteEntryRequest.newBuilder()
                    .setNamespace(namespace)
                    .setSet(set)
                    .setEntry(entry)
                    .build();
            return getStub().deleteEntry(request).getDeleted();
        });
    }

    @Override
    public void delete(final String namespace, final String set, final Collection<String> entries) throws IOException {
        checkDelete(namespace, set, entries);
        call(() -> {
            final DeleteBatchRequest request = DeleteBatchRequest.newBuilder()
                    .setNamespace(namespace)
                    .setSet(set)
                    .addAllEntries(entries)
                    .build();
            getStub().deleteBatch(request);
            return null;
        });
    }

    @Override
    public Collection<String> entries(final String namespace, final String set, final long min, final long max, final int start, final int count, final boolean ascending) throws IOException {
        checkKeys(namespace, set, min, max, start, count, ascending);
        return call(() -> {
            final KeysRequest request = KeysRequest.newBuilder()
                    .setNamespace(namespace)
                    .setSet(set)
                    .setMin(min)
                    .setMax(max)
                    .setStart(start)
                    .setCount(count)
                    .setAscending(ascending)
                    .build();
            return getStub().keys(request).getKeysList();
        });
    }

    @Override
    public Collection<String> sets(final String namespace) throws IOException {
        checkSets(namespace);
        return call(() -> {
            final SetsRequest request = SetsRequest.newBuilder()
                    .setNamespace(namespace)
                    .build();
            return getStub().sets(request).getSetsList();
        });
    }

    @Override
    public int size(final String namespace, final String set) throws IOException {
        checkSize(namespace, set);
        return call(() -> {
            final SizeRequest request = SizeRequest.newBuilder()
                    .setNamespace(namespace)
                    .setSet(set)
                    .build();
            return getStub().size(request).getSize();
        });
    }

    @Override
    public Long weight(final String namespace, final String set, final String entry) throws IOException {
        checkWeight(namespace, set, entry);
        return call(() -> {
            final WeightRequest request = WeightRequest.newBuilder()
                    .setNamespace(namespace)
                    .setSet(set)
                    .setEntry(entry)
                    .build();
            final WeightResponse response = getStub().weight(request);
            return response.getFound() ? response.getWeight() : null;
        });
    }

    @Override
    public long inc(final String namespace, final String set, final String entry, final long count) throws IOException {
        checkInc(namespace, set, entry, count);
        return call(() -> {
            final IncRequest request = IncRequest.newBuilder()
                    .setNamespace(namespace)
                    .setSet(set)
                    .setEntry(entry)
                    .setCount(count)
                    .build();
            final IncResponse response = getStub().inc(request);
            return response.getResult();
        });
    }
}

