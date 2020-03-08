/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.google.protobuf.ByteString;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.grpc.objects.*;
import com.salesforce.cantor.grpc.objects.ObjectsServiceGrpc.ObjectsServiceBlockingStub;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.common.CommonPreconditions.checkString;
import static com.salesforce.cantor.common.ObjectsPreconditions.*;

public class ObjectsOnGrpc extends AbstractBaseGrpcClient<ObjectsServiceBlockingStub> implements Objects {

    public ObjectsOnGrpc(final String target) {
        super(ObjectsServiceGrpc::newBlockingStub, target);
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
    public Collection<String> keys(final String namespace, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count);
        return call(() -> {
            final KeysRequest request = KeysRequest.newBuilder()
                    .setNamespace(namespace)
                    .setStart(start)
                    .setCount(count)
                    .build();
            final KeysResponse response = getStub().keys(request);
            return response.getKeysList();
        });
    }

    @Override
    public void store(final String namespace, final String key, final byte[] bytes) throws IOException {
        checkString(key, "null/empty key");
        checkArgument(bytes != null, "null bytes");
        call(() -> {
            final StoreRequest request = StoreRequest.newBuilder()
                    .setNamespace(namespace)
                    .setKey(key)
                    .setValue(ByteString.copyFrom(bytes))
                    .build();
            getStub().store(request);
            return null;
        });
    }

    @Override
    public void store(final String namespace, final Map<String, byte[]> batch) throws IOException {
        checkArgument(batch != null, "null batch");
        call(() -> {
            for (final Map.Entry<String, byte[]> entry : batch.entrySet()) {
                store(namespace, entry.getKey(), entry.getValue());
            }
            return null;
        });
    }

    @Override
    public byte[] get(final String namespace, final String key) throws IOException {
        checkString(key, "null/empty key");
        return call(() -> {
            final GetRequest request = GetRequest.newBuilder()
                    .setNamespace(namespace)
                    .setKey(key)
                    .build();
            final GetResponse response = getStub().get(request);
            if (response == null || response.getValue() == null) {
                return null;
            }
            final byte[] results = response.getValue().toByteArray();
            if (results.length == 0) {
                return null;
            }
            return results;
        });
    }

    @Override
    public Map<String, byte[]> get(final String namespace, final Collection<String> keys) throws IOException {
        checkArgument(keys != null, "null entries");
        return call(() -> {
            final Map<String, byte[]> results = new HashMap<>();
            for (final String key : keys) {
                results.put(key, get(namespace, key));
            }
            return results;
        });
    }

    @Override
    public boolean delete(final String namespace, final String key) throws IOException {
        checkString(key, "null/empty key");
        return call(() -> {
            final DeleteRequest request = DeleteRequest.newBuilder()
                    .setNamespace(namespace)
                    .setKey(key)
                    .build();
            return getStub().delete(request).getResult();
        });
    }

    @Override
    public void delete(final String namespace, final Collection<String> keys) throws IOException {
        checkArgument(keys != null, "null entries");
        call(() -> {
            for (final String key : keys) {
                delete(namespace, key);
            }
            return null;
        });
    }

    @Override
    public int size(final String namespace) throws IOException {
        return call(() -> {
            final SizeRequest request = SizeRequest.newBuilder()
                    .setNamespace(namespace)
                    .build();
            return getStub().size(request).getSize();
        });
    }
}

