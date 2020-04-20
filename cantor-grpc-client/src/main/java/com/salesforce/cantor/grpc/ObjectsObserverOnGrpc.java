/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.google.protobuf.*;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.grpc.objects.*;
import com.salesforce.cantor.h2.ObjectsOnH2;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;

public class ObjectsObserverOnGrpc extends AbstractBaseGrpcClient<ObjectsStreamGrpc.ObjectsStreamStub> {

    private final Logger logger = LoggerFactory.getLogger(ObjectsObserverOnGrpc.class);
    private final AtomicReference<StreamObserver<StreamObject>> outStream = new AtomicReference<>();
    private final AtomicReference<StreamObserver<StreamObject>> inStream = new AtomicReference<>();
    private final ProtobufRequestHandlers handlers = null;
    private final Objects delegate;

    public ObjectsObserverOnGrpc(final String target, final Objects delegate) {
        super(ObjectsStreamGrpc::newStub, target);
        this.delegate = delegate;
        // calling observe
        this.inStream.set(new StreamObserver<StreamObject>() {
            @Override
            public void onNext(final StreamObject streamObject) {
                logger.info("received this thing: {}", streamObject);
                try {
                    final Message request = toProtobuf(streamObject);
                    if (request == null) {
                        logger.warn("null request: {}", streamObject);
                        return;
                    }
                    logger.info("request class is: {}  and content is: {}", request.getClass().getName(), request);
                    final Message response = handlers.handle(request);
                    sendResponse(wrap(streamObject.getGuid(), response));
                } catch (IOException e) {
                    logger.warn("failed", e);
                }
            }

            @Override
            public void onError(final Throwable throwable) {
                logger.warn("error - reconnecting", throwable);
                outStream.set(getStub().register(inStream.get()));
            }

            @Override
            public void onCompleted() {
                logger.warn("completed");
            }
        });
        this.outStream.set(getStub().register(this.inStream.get()));
    }

    private StreamObject wrap(final String requestId, final Message message) {
        return StreamObject.newBuilder()
                .setGuid(requestId)
                .setClassName(message.getClass().getSimpleName())
                .setPayload(ByteString.copyFrom(message.toByteArray()))
                .build();
    }

    private Objects getDelegate() {
        return this.delegate;
    }

    private void sendResponse(final StreamObject response) {
        this.outStream.get().onNext(response);
    }

    private Message toProtobuf(final StreamObject object) {
        try {
            final String className = object.getClassName();
            final Class<? extends Message> protobufClass = (Class<? extends Message>) Class.forName(className);
            final Method method = protobufClass.getMethod("parseFrom", byte[].class);
            return (Message) method.invoke(null, object.getPayload().toByteArray());
        } catch (Exception e) {
            logger.warn("exception", e);
            return null;
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        final Objects objects = new ObjectsOnH2("/tmp/fooo");
        objects.create("bang-boong-dang");
        final ObjectsObserverOnGrpc observer = new ObjectsObserverOnGrpc("localhost:9999", objects);
        Thread.sleep(10000000);
    }
}

