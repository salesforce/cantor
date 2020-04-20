/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.grpc.objects.*;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class ObservableObjects extends ObjectsStreamGrpc.ObjectsStreamImplBase implements Objects {
    private static final Logger logger = LoggerFactory.getLogger(ObservableObjects.class);

    private final AtomicReference<StreamObserver<StreamObject>> observerReference = new AtomicReference<>();
    private final Map<String, StreamObject> requestToResponse = new ConcurrentHashMap<>();
    private final Map<String, Object> requestLock = new ConcurrentHashMap<>();

    @Override
    public StreamObserver<StreamObject> register(final StreamObserver<StreamObject> responseObserver) {
        logger.info("setting the observer reference...");
        observerReference.set(responseObserver);
        return new StreamObserver<StreamObject>() {
            @Override
            public void onNext(final StreamObject response) {
                requestToResponse.put(response.getGuid(), response);
                synchronized (requestLock.get(response.getGuid())) {
                    requestLock.get(response.getGuid()).notify();
                }
                requestLock.remove(response.getGuid());
            }

            @Override
            public void onError(final Throwable throwable) {
                observerReference.set(null);
                // TODO remove from namespace to observer mapping
            }

            @Override
            public void onCompleted() {
                // TODO remove from namespace to observer mapping
            }
        };
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        final String requestId = UUID.randomUUID().toString();
        if (this.observerReference.get() == null) {
            logger.info("did not find a node");
            return Collections.emptyList();
        }
        this.observerReference.get().onNext(
                wrap(requestId, NamespacesRequest.newBuilder().build())
        );
        this.requestLock.put(requestId, new Object[0]);
        try {
            synchronized (this.requestLock.get(requestId)) {
                this.requestLock.get(requestId).wait();
            }
        } catch (InterruptedException e) {
            logger.warn("timeout", e);
        }
        final NamespacesResponse response = NamespacesResponse.parseFrom(
                this.requestToResponse.get(requestId).getPayload().toByteArray()
        );
        return response.getNamespacesList();
    }

    @Override
    public void create(String namespace) throws IOException {

    }

    @Override
    public void drop(String namespace) throws IOException {

    }

    @Override
    public void store(String namespace, String key, byte[] bytes) throws IOException {

    }

    @Override
    public byte[] get(String namespace, String key) throws IOException {
        return new byte[0];
    }

    @Override
    public boolean delete(String namespace, String key) throws IOException {
        return false;
    }

    @Override
    public Collection<String> keys(String namespace, int start, int count) throws IOException {
        return null;
    }

    @Override
    public int size(String namespace) throws IOException {
        return 0;
    }

    private StreamObject wrap(final String requestId, final AbstractMessage message) {
        return StreamObject.newBuilder()
                .setGuid(requestId)
                .setClassName(message.getClass().getName())
                .setPayload(ByteString.copyFrom(message.toByteArray()))
                .build();
    }













    public static void main(final String[] args) throws IOException, InterruptedException {
//        final Cantor cantor = new CantorOnH2("/tmp/foooo");
        final ObservableObjects objects = new ObservableObjects();
        final Server server = NettyServerBuilder.forPort(9999)
                .workerEventLoopGroup(new NioEventLoopGroup(
                        8,  // max of exactly 8 event loop threads
                        new ThreadFactoryBuilder().setNameFormat("cantor-grpc-event-loop-%d").build())
                )
                .maxMessageSize(64 * 1024 * 1024) // 64MB
                .addService(objects)
                .executor(
                        Executors.newFixedThreadPool(
                                64, // exactly 64 concurrent worker threads
                                new ThreadFactoryBuilder().setNameFormat("cantor-grpc-worker-%d").build())
                )
                .build();
        server.start();

        while (true) {
            try {
                Thread.sleep(1000);
                logger.info("doing namespaces...");
                logger.info("namespace: {}", objects.namespaces());
            } catch (Throwable throwable) {
                logger.warn("error: ", throwable);
            }
        }
    }
}
