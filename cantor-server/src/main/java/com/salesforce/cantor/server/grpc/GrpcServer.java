/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.server.grpc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.grpc.EventsGrpcService;
import com.salesforce.cantor.grpc.ObjectsGrpcService;
import com.salesforce.cantor.grpc.SetsGrpcService;
import com.salesforce.cantor.server.CantorEnvironment;
import com.salesforce.cantor.server.Constants;
import com.salesforce.cantor.server.utils.CantorFactory;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

public class GrpcServer {
    private static final Logger logger = LoggerFactory.getLogger(GrpcServer.class);

    private final Server server;

    public GrpcServer(final CantorEnvironment cantorEnvironment) throws IOException {
        final CantorFactory cantorProvider = new CantorFactory(cantorEnvironment);

        final int port = cantorEnvironment.getConfigAsInteger(Constants.CANTOR_PORT_GRPC, -1);
        logger.info("creating new grpc server listening on port '{}' with storage type: '{}'",
                port,
                cantorEnvironment.getStorageType()
        );

        final Cantor cantor = cantorProvider.getCantor();
        this.server = ServerBuilder.forPort(port)
                .maxInboundMessageSize(64 * 1024 * 1024) // 64MB
                .addService(new ObjectsGrpcService(cantor))
                .addService(new SetsGrpcService(cantor))
                .addService(new EventsGrpcService(cantor))
                .executor(
                        Executors.newFixedThreadPool(
                                64, // exactly 64 concurrent worker threads
                                new ThreadFactoryBuilder().setNameFormat("cantor-grpc-worker-%d").build())
                )
                .build();

        addShutdownHook();
    }

    public CompletableFuture<?> start() {
        return CompletableFuture.runAsync(() -> {
            try {
                Thread.currentThread().setName("cantor-grpc-server");
                logger.info("starting grpc server...");
                this.server.start();
                blockUntilShutdown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void shutdown() {
        this.server.shutdown();
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(GrpcServer.this::stop));
    }

    private void stop() {
        if (this.server != null) {
            this.server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (this.server != null) {
            this.server.awaitTermination();
        }
    }

}

