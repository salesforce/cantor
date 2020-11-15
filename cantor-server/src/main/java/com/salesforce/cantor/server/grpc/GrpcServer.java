/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.server.grpc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.grpc.*;
import com.salesforce.cantor.grpc.auth.AuthorizationInterceptor;
import com.salesforce.cantor.server.CantorEnvironment;
import com.salesforce.cantor.server.Constants;
import com.salesforce.cantor.server.utils.CantorFactory;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
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
        final NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(port)
                .workerEventLoopGroup(new NioEventLoopGroup(
                        8,  // max of exactly 8 event loop threads
                        new ThreadFactoryBuilder().setNameFormat("cantor-grpc-event-loop-%d").build())
                )
                .maxMessageSize(64 * 1024 * 1024) // 64MB
                .addService(new ObjectsGrpcService(cantor))
                .addService(new SetsGrpcService(cantor))
                .addService(new EventsGrpcService(cantor))
                .addService(new AuthorizationGrpcService(cantor))
                .intercept(new AuthorizationInterceptor(cantor))
                .executor(
                        Executors.newFixedThreadPool(
                                64, // exactly 64 concurrent worker threads
                                new ThreadFactoryBuilder().setNameFormat("cantor-grpc-worker-%d").build())
                );

        final String certPath = cantorEnvironment.getEnvironmentVariable("CERTS_PATH");
        if (certPath != null) {
            serverBuilder.useTransportSecurity(
                Paths.get(certPath, "/certificates/server.pem").toFile(),
                Paths.get(certPath, "/keys/server-key.pem").toFile()
            );
        }

        this.server = serverBuilder.build();
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

