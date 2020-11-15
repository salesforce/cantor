/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.google.protobuf.Message;
import com.salesforce.cantor.grpc.auth.AuthorizationInterceptor;
import com.salesforce.cantor.grpc.auth.Roles;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GrpcUtils {
    private static final Logger logger = LoggerFactory.getLogger(GrpcUtils.class);

    static void sendError(final StreamObserver<?> observer, final Throwable throwable) {
        logger.warn("exception caught handling request: ", throwable);
        observer.onError(new StatusRuntimeException(Status.INTERNAL.withCause(throwable)
                .withDescription(throwable.getMessage())));
    }

    static void sendCancelledError(final StreamObserver<?> observer, final Throwable cancellationCause) {
        logger.warn("request is cancelled by client: ", cancellationCause);
        observer.onError(Status.CANCELLED.withDescription("request is cancelled by client").asRuntimeException());
    }

    static <T extends Message> void sendResponse(final StreamObserver<T> observer, final T t) {
        observer.onNext(t);
        observer.onCompleted();
    }

    static boolean writeRequestValid(final String namespace) {
        final Roles roles = AuthorizationInterceptor.userRoles.get(Context.current());
        return roles != null && roles.hasWriteAccess(namespace);
    }

    static boolean readRequestValid(final String namespace) {
        final Roles roles = AuthorizationInterceptor.userRoles.get(Context.current());
        return roles != null && roles.hasReadAccess(namespace);
    }
}
