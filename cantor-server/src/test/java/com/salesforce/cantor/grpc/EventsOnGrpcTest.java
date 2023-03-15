/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.common.AbstractBaseEventsTest;
import com.salesforce.cantor.server.grpc.GrpcServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

import static org.testng.Assert.assertEquals;

public class EventsOnGrpcTest extends AbstractBaseEventsTest {
    private GrpcServer server = new GrpcServer(GrpcTests.getTestCantorProperties());

    public EventsOnGrpcTest() throws IOException {
    }

    @BeforeClass
    private void start() {
        this.server.start();
    }

    @AfterClass
    private void stop() {
        this.server.shutdown();
    }

    @Override
    public Cantor getCantor() {
        return GrpcTests.getCantor();
    }

    @Test
    public void testStoreBatch() throws IOException {
        final int batchSize = 10;
        final int totalNamespaceCount = 10;
        final long startTimestamp = System.currentTimeMillis();
        final Map<String, Collection<Events.Event>> eventBatch = new HashMap<>();
        for (int n = 0; n < totalNamespaceCount; ++n) {
            final String namespace = UUID.randomUUID().toString();
            final List<Events.Event> batch = new ArrayList<>();
            for (int b = 0; b < batchSize; ++b) {
                final long timestamp = startTimestamp + b;
                final Map<String, String> metadata = getRandomMetadata(100);
                final Map<String, Double> dimensions = getRandomDimensions(100);
                final byte[] payload = getRandomPayload(8 * 1024);
                batch.add(new Events.Event(timestamp, metadata, dimensions, payload));
            }
            logger.info("namespace: {} batch size: {}", namespace, batch.size());
            eventBatch.put(namespace, batch);
        }

        // first create all namespaces
        for (final String namespace : eventBatch.keySet()) {
            getEvents().create(namespace);
        }

        // store the batch
        ((EventsOnGrpc)getEvents()).store(eventBatch);

        // verify
        for (final String namespace : eventBatch.keySet()) {
            final List<Events.Event> results = getEvents().get(
                    namespace,
                    startTimestamp, startTimestamp + batchSize,
                    true
            );
            logger.info("results.size: {} - batch size: {}", results.size(), batchSize);
//            assertEquals(results.size(), batchSize);
//            assertEquals(results, eventBatch.get(namespace));
        }
    }
}

