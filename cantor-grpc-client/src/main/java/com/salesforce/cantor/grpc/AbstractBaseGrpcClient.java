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
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static com.salesforce.cantor.common.CommonPreconditions.checkString;

abstract class AbstractBaseGrpcClient<StubType extends AbstractStub<StubType>> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final StubType stub;

    AbstractBaseGrpcClient(final Function<Channel, StubType> stubConstructor,
                           final String target) {
        checkString(target, "null/empty target");
        this.stub = makeStubs(stubConstructor, target);
    }

    AbstractBaseGrpcClient(final Function<Channel, StubType> stubConstructor,
                           final String target,
                           final String jwtSigningKey) {
        checkString(target, "null/empty target");
        checkString(jwtSigningKey, "null/empty jwtSigningKey");
        this.stub = makeSecureStubs(stubConstructor, target, jwtSigningKey);
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

    private StubType makeStubs(final Function<Channel, StubType> stubConstructor,
                               final String target) {
        logger.info("creating stub of {} for target '{}'", stubConstructor.getClass(), target);
        final ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext(true)
                .maxInboundMessageSize(32 * 1024 * 1024)  // 32MB
                .executor(
                        Executors.newFixedThreadPool(
                                16, // exactly 16 concurrent worker threads
                                new ThreadFactoryBuilder().setNameFormat("cantor-client-channel-%d").build())
                )
                .build();
        return stubConstructor
                .apply(channel);
    }

    private StubType makeSecureStubs(final Function<Channel, StubType> stubConstructor,
                                     final String target,
                                     final String jwtSigningKey) {
        logger.info("creating stub of {} for target '{}'", stubConstructor.getClass(), target);
        final ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext(true)
                .maxInboundMessageSize(32 * 1024 * 1024)  // 32MB
                .executor(
                        Executors.newFixedThreadPool(
                                16, // exactly 16 concurrent worker threads
                                new ThreadFactoryBuilder().setNameFormat("cantor-client-channel-%d").build())
                )
                .build();
        return stubConstructor
                .apply(channel)
                .withCallCredentials(new BearerToken(getJwt(jwtSigningKey)));
    }

    protected String getJwt(final String jwtSigningKey) {
        return Jwts.builder()
                .setSubject("GreetingClient")
                .signWith(SignatureAlgorithm.HS256, jwtSigningKey)
                .compact();
    }
}
