/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static com.salesforce.cantor.common.CommonPreconditions.checkString;

abstract class AbstractBaseGrpcClient<StubType extends AbstractStub<StubType>> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Channel channel;
    private final String target;
    private final StubType stub;
    private final Function<Channel, StubType> stubConstructor;

    AbstractBaseGrpcClient(final Function<Channel, StubType> stubConstructor,
                           final String target) {
        checkString(target, "null/empty target");
        this.target = target;
        this.stubConstructor = stubConstructor;

        this.channel = makeChannel();
        this.stub = makeStubs();

        // redirect JUL to slf4j
        SLF4JBridgeHandler.install();
    }

    StubType getStub() {
        return this.stub;
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

    private Channel makeChannel() {
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
        return this.stubConstructor.apply(this.channel);
    }
}
