/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.salesforce.cantor.Maps;
import com.salesforce.cantor.grpc.maps.*;
import com.salesforce.cantor.grpc.maps.MapsServiceGrpc.MapsServiceBlockingStub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static com.salesforce.cantor.common.CommonPreconditions.checkCreate;
import static com.salesforce.cantor.common.CommonPreconditions.checkDrop;
import static com.salesforce.cantor.common.MapsPreconditions.checkGet;
import static com.salesforce.cantor.common.MapsPreconditions.checkStore;

public class MapsOnGrpc extends AbstractBaseGrpcClient<MapsServiceBlockingStub> implements Maps {

    public MapsOnGrpc(final String target) {
        super(MapsServiceGrpc::newBlockingStub, target);
    }

    public MapsOnGrpc(final String target, final long timeoutMillis) {
        super(MapsServiceGrpc::newBlockingStub, target, timeoutMillis);
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
            return getStub().create(request);
        });
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        call(() -> {
            final DropRequest request = DropRequest.newBuilder()
                    .setNamespace(namespace)
                    .build();
            return getStub().drop(request);
        });
    }

    @Override
    public void store(final String namespace, final Map<String, String> map) throws IOException {
        checkStore(namespace, map);
        call(() -> {
            final StoreRequest request = StoreRequest.newBuilder()
                    .setNamespace(namespace)
                    .putAllMap(map)
                    .build();
            return getStub().store(request);
        });
    }

    @Override
    public Collection<Map<String, String>> get(final String namespace, final Map<String, String> query) throws IOException {
        checkGet(namespace, query);
        return call(() -> {
            final GetRequest request = GetRequest.newBuilder()
                    .setNamespace(namespace)
                    .putAllQuery(query)
                    .build();
            final Collection<Map<String, String>> results = new ArrayList<>();
            final GetResponse response = getStub().get(request);
            for (final MapProto map : response.getResultsList()) {
                results.add(map.getMapMap());
            }
            return results;
        });
    }

    @Override
    public int delete(final String namespace, final Map<String, String> query) throws IOException {
        checkStore(namespace, query);
        return call(() -> {
            final DeleteRequest request = DeleteRequest.newBuilder()
                    .setNamespace(namespace)
                    .putAllQuery(query)
                    .build();
            return getStub().delete(request).getResults();
        });
    }
}

