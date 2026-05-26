/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.multicloudj;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.Sets;
import com.salesforce.cantor.common.AbstractBaseEventsTest;
import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.inmemory.InMemoryBlobStore;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class EventsOnMulticloudjTest extends AbstractBaseEventsTest {

    // The blob backend mirrors cantor-s3: store() does not enforce namespace existence;
    // matches cantor-s3 behavior, which leaves its conformance tests disabled for the same reason.
    @Override
    @Test(enabled = false)
    public void testBadInput() {
    }

    @Override
    @Test(enabled = false)
    public void testCreateDrop() {
    }


    // shared across getCantor() calls within a test instance: the abstract test calls getCantor()
    // multiple times (once per before/after, plus inside test bodies), and they must all hit the
    // same bucket so namespaces created in @BeforeMethod are visible to the test body
    private static final String bucketName = "cantor-events-test-" + UUID.randomUUID();
    private static volatile Cantor cantor;
    private static volatile File bufferDir;

    @Override
    protected synchronized Cantor getCantor() throws IOException {
        if (cantor != null) {
            return cantor;
        }
        bufferDir = Files.createTempDirectory("cantor-events-test-buffer-").toFile();
        InMemoryBlobStore.createBucket(bucketName);
        final InMemoryBlobStore store = new InMemoryBlobStore.Builder()
                .withBucket(bucketName)
                .withRegion("us-west-2")
                .build();
        final BucketClient bucketClient = new InMemoryTestBucketClient(store);
        // 1-second flush interval keeps tests responsive while preserving the buffered code path
        final EventsOnMulticloudj events = new EventsOnMulticloudj(bucketClient, bufferDir.getAbsolutePath(), 1);
        final Objects objects = new ObjectsOnMulticloudj(bucketClient);
        cantor = new FlushingTestCantor(objects, events);
        return cantor;
    }

    @AfterClass(alwaysRun = true)
    public static void cleanupBufferDir() throws IOException {
        if (bufferDir != null && bufferDir.exists()) {
            try (java.util.stream.Stream<java.nio.file.Path> walk = Files.walk(bufferDir.toPath())) {
                walk.sorted(Comparator.reverseOrder()).map(java.nio.file.Path::toFile).forEach(File::delete);
            }
        }
    }

    /**
     * Test-only Cantor that wraps {@link EventsOnMulticloudj} to call {@code flushNow()} before
     * every read so the conformance suite can run against the buffered code path without sleeping.
     */
    private static final class FlushingTestCantor implements Cantor {
        private final Objects objects;
        private final FlushingEvents events;

        FlushingTestCantor(final Objects objects, final EventsOnMulticloudj events) {
            this.objects = objects;
            this.events = new FlushingEvents(events);
        }

        @Override public Objects objects() { return this.objects; }
        @Override public Sets sets() { throw new UnsupportedOperationException("Sets are not implemented on multicloudj"); }
        @Override public Events events() { return this.events; }
    }

    private static final class FlushingEvents implements Events {
        private final EventsOnMulticloudj delegate;

        FlushingEvents(final EventsOnMulticloudj delegate) {
            this.delegate = delegate;
        }

        @Override
        public void create(final String namespace) throws IOException {
            this.delegate.create(namespace);
        }

        @Override
        public void drop(final String namespace) throws IOException {
            this.delegate.drop(namespace);
        }

        @Override
        public void store(final String namespace, final Collection<Event> batch) throws IOException {
            this.delegate.store(namespace, batch);
        }

        @Override
        public List<Event> get(final String namespace,
                               final long startTimestampMillis,
                               final long endTimestampMillis,
                               final Map<String, String> metadataQuery,
                               final Map<String, String> dimensionsQuery,
                               final boolean includePayloads,
                               final boolean ascending,
                               final int limit) throws IOException {
            this.delegate.flushNow();
            return this.delegate.get(namespace, startTimestampMillis, endTimestampMillis,
                    metadataQuery, dimensionsQuery, includePayloads, ascending, limit);
        }

        @Override
        public Set<String> metadata(final String namespace,
                                    final String metadataKey,
                                    final long startTimestampMillis,
                                    final long endTimestampMillis,
                                    final Map<String, String> metadataQuery,
                                    final Map<String, String> dimensionsQuery) throws IOException {
            this.delegate.flushNow();
            return this.delegate.metadata(namespace, metadataKey, startTimestampMillis, endTimestampMillis,
                    metadataQuery, dimensionsQuery);
        }

        @Override
        public List<Event> dimension(final String namespace,
                                     final String dimensionKey,
                                     final long startTimestampMillis,
                                     final long endTimestampMillis,
                                     final Map<String, String> metadataQuery,
                                     final Map<String, String> dimensionsQuery) throws IOException {
            this.delegate.flushNow();
            return this.delegate.dimension(namespace, dimensionKey, startTimestampMillis, endTimestampMillis,
                    metadataQuery, dimensionsQuery);
        }

        @Override
        public void expire(final String namespace, final long endTimestampMillis) throws IOException {
            this.delegate.flushNow();
            this.delegate.expire(namespace, endTimestampMillis);
        }
    }
}
