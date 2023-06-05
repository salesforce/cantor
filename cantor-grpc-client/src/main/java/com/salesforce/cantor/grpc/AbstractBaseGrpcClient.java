/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.*;
import io.grpc.stub.AbstractStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.salesforce.cantor.common.CommonPreconditions.checkString;

abstract class AbstractBaseGrpcClient<StubType extends AbstractStub<StubType>> {
    private static final long defaultChannelRefreshTimeMillis = 10 * 60 * 1000;  // 10 minutes
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final AtomicReference<ManagedChannel> channel = new AtomicReference<>();
    private final AtomicReference<ManagedChannel> oldChannel = new AtomicReference<>();
    private final String target;
    private final Function<Channel, StubType> stubConstructor;

    AbstractBaseGrpcClient(final Function<Channel, StubType> stubConstructor,
                           final String target) {
        checkString(target, "null/empty target");
        this.target = target;
        this.stubConstructor = stubConstructor;

        // redirect JUL to slf4j
        SLF4JBridgeHandler.install();

        // refresh periodically to ensure channels are connected to real servers (and load-balance)
        refreshGrpcChannel();
        final ScheduledExecutorService refreshChannelExecutorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("cantor-grpc-refresh-channel-%d").build());
        refreshChannelExecutorService.scheduleWithFixedDelay(this::refreshGrpcChannel,
                defaultChannelRefreshTimeMillis, defaultChannelRefreshTimeMillis, TimeUnit.MILLISECONDS);
    }

    StubType getStub() {
        // create a new stub with deadline of 1 minute
        return makeStubs().withDeadlineAfter(60, TimeUnit.SECONDS);
    }

    <R> R call(final Callable<R> callable) throws IOException {
        try {
            return callable.call();
        } catch (StatusRuntimeException e) {
            if (e.getStatus() != null && e.getStatus().getCause() != null) {
                // try to forward the cause
                throw new IOException(e.getStatus().getCause());
            } else {
                throw new IOException(e);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private ManagedChannel makeChannel() {
        logger.info("creating channel for target '{}'", this.target);
        return ManagedChannelBuilder.forTarget(this.target)
                .usePlaintext()
                .maxInboundMessageSize(64 * 1024 * 1024)  // 64MB
                .executor(Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder().setNameFormat("cantor-grpc-client-%d").build())
                )
                .build();
    }

    private StubType makeStubs() {
        return this.stubConstructor.apply(this.channel.get());
    }

    private void refreshGrpcChannel() {
        logger.info("refreshing grpc channel at timeInMillis={} on refreshInterval={}", System.currentTimeMillis(), defaultChannelRefreshTimeMillis);
        final ManagedChannel channelToClose = this.oldChannel.getAndSet(this.channel.getAndSet(makeChannel()));
        if (channelToClose != null) {
            channelToClose.shutdown();
        }
    }
}
