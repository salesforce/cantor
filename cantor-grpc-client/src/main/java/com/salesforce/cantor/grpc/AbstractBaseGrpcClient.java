/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import io.grpc.*;
import io.grpc.stub.AbstractStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractBaseGrpcClient<StubType extends AbstractStub<StubType>> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final StubType stub;

    protected AbstractBaseGrpcClient(final Function<Channel, StubType> stubConstructor,
                                     final String target) {
        this.stub = makeStubs(stubConstructor, GrpcChannelBuilder.newBuilder(target).usePlainText().build());
    }

    protected AbstractBaseGrpcClient(final Function<Channel, StubType> stubConstructor,
                                     final ManagedChannel channel) {
        checkNotNull(channel, "null channel");
        this.stub = makeStubs(stubConstructor, channel);
    }

    protected StubType getStub() {
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

    private StubType makeStubs(final Function<Channel, StubType> stubConstructor,
                               final ManagedChannel channel) {
        logger.info("creating stub of {} for target '{}'", stubConstructor.getClass(), channel.authority());
        return stubConstructor
                .apply(channel);
    }
}
