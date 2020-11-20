/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.google.protobuf.Message;
import com.salesforce.cantor.management.Users;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.salesforce.cantor.grpc.auth.UserUtils.getCurrentUser;

public class GrpcUtils {
    private static final Logger logger = LoggerFactory.getLogger(GrpcUtils.class);

    public static void sendError(final StreamObserver<?> observer, final Throwable throwable) {
        logger.warn("exception caught handling request: ", throwable);
        observer.onError(new StatusRuntimeException(Status.INTERNAL.withCause(throwable)
                .withDescription(throwable.getMessage())));
    }

    public static void sendUnauthorizedError(final StreamObserver<?> observer, final String request) {
        final Users.User currentUser = getCurrentUser();
        logger.warn("unauthorized request: user={} status={} request={}", currentUser.getUsername(), currentUser.getStatus(), request);
        observer.onError(new StatusRuntimeException(Status.ABORTED.withDescription(String.format("User not authorized to make this request: user=%s status=%s request=%s", currentUser.getUsername(), currentUser.getStatus(), request))));
    }

    public static void sendCancelledError(final StreamObserver<?> observer, final Throwable cancellationCause) {
        logger.warn("request is cancelled by client: ", cancellationCause);
        observer.onError(Status.CANCELLED.withDescription("request is cancelled by client").asRuntimeException());
    }

    public static <T extends Message> void sendResponse(final StreamObserver<T> observer, final T t) {
        observer.onNext(t);
        observer.onCompleted();
    }
}
