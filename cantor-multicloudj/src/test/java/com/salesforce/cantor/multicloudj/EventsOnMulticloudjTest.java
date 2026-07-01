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
import com.salesforce.cantor.multicloudj.support.InMemoryBucketClients;
import com.salesforce.multicloudj.blob.client.BucketClient;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.*;

import static org.testng.Assert.*;

public class EventsOnMulticloudjTest extends AbstractBaseEventsTest {
    private final Cantor cantor;
    private final EventsOnMulticloudj eventsImpl;

    public EventsOnMulticloudjTest() throws IOException {
        BucketClient bc = InMemoryBucketClients.fresh();
        this.eventsImpl = new EventsOnMulticloudj(bc, createTempBufferDir(), 1);
        this.cantor = new TestCantor(bc, eventsImpl);
    }

    @Override
    protected Cantor getCantor() throws IOException {
        return cantor;
    }

    @Test
    public void testGetMatchingKeys_rejectsStartAfterEnd() throws IOException {
        final Events events = cantor.events();
        final String ns = "reject-start-after-end-" + UUID.randomUUID();
        events.create(ns);
        assertThrows(IllegalArgumentException.class,
                () -> events.get(ns, 20, 10, null, null));
        events.drop(ns);
    }

    @Test
    public void testIncludePayloads_roundTrips() throws IOException {
        final Events events = cantor.events();
        final String ns = "payload-roundtrip-" + UUID.randomUUID();
        events.create(ns);

        byte[] payload = "hello-payload-test".getBytes();
        long ts = System.currentTimeMillis();
        Map<String, String> meta = Collections.singletonMap("k", "v");
        events.store(ns, ts, meta, null, payload);
        eventsImpl.forceFlushNow();

        List<Events.Event> results = events.get(ns, ts, ts + 1, true);
        assertEquals(results.size(), 1);
        assertEquals(results.get(0).getPayload(), payload);

        events.drop(ns);
    }

    @Test
    public void testTwoInstances_dontShareState() throws IOException {
        Path bufferDir = createTempBufferDirPath();
        BucketClient bc = InMemoryBucketClients.fresh();

        EventsOnMulticloudj events1 = new EventsOnMulticloudj(bc, bufferDir.toString(), 60);
        EventsOnMulticloudj events2 = new EventsOnMulticloudj(bc, bufferDir.toString(), 60);

        String ns = "shared-test-ns-" + UUID.randomUUID();
        events1.create(ns);

        long ts = System.currentTimeMillis();
        events1.store(ns, Collections.singleton(
                new Events.Event(ts, Collections.singletonMap("source", "inst1"), null)));
        events2.store(ns, Collections.singleton(
                new Events.Event(ts + 1, Collections.singletonMap("source", "inst2"), null)));

        events1.forceFlushNow();
        events2.forceFlushNow();

        List<Events.Event> all = events1.get(ns, ts, ts + 2, null, null, false, true, 0);
        assertEquals(all.size(), 2);

        events1.close();
        events2.close();
    }

    @Test
    public void testStore_concurrentWritesDifferentNamespaces() throws Exception {
        BucketClient bc = InMemoryBucketClients.fresh();
        EventsOnMulticloudj events = new EventsOnMulticloudj(bc, createTempBufferDir(), 60);

        String ns1 = "concurrent-ns1-" + UUID.randomUUID();
        String ns2 = "concurrent-ns2-" + UUID.randomUUID();
        events.create(ns1);
        events.create(ns2);

        long ts = System.currentTimeMillis();
        int count = 50;

        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            final int idx = i;
            futures.add(executor.submit(() -> {
                try {
                    events.store(ns1, Collections.singleton(
                            new Events.Event(ts + idx, Collections.singletonMap("i", String.valueOf(idx)), null)));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }));
            futures.add(executor.submit(() -> {
                try {
                    events.store(ns2, Collections.singleton(
                            new Events.Event(ts + idx, Collections.singletonMap("i", String.valueOf(idx)), null)));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        executor.shutdown();

        events.forceFlushNow();

        List<Events.Event> results1 = events.get(ns1, ts, ts + count, null, null, false, true, 0);
        List<Events.Event> results2 = events.get(ns2, ts, ts + count, null, null, false, true, 0);

        assertEquals(results1.size(), count);
        assertEquals(results2.size(), count);

        events.close();
    }

    // ─── Constructor validation tests ────────────────────────────────────────────

    @Test
    public void testConstructor_rejectsZeroFlushInterval() {
        BucketClient bc = InMemoryBucketClients.fresh();
        assertThrows(IllegalArgumentException.class,
                () -> new EventsOnMulticloudj(bc, createTempBufferDir(), 0));
    }

    @Test
    public void testConstructor_rejectsNegativeFlushInterval() {
        BucketClient bc = InMemoryBucketClients.fresh();
        assertThrows(IllegalArgumentException.class,
                () -> new EventsOnMulticloudj(bc, createTempBufferDir(), -1));
    }

    @Test
    public void testConstructor_rejectsNullBufferDirectory() {
        BucketClient bc = InMemoryBucketClients.fresh();
        assertThrows(IllegalArgumentException.class,
                () -> new EventsOnMulticloudj(bc, null, 60));
    }

    @Test
    public void testConstructor_rejectsEmptyBufferDirectory() {
        BucketClient bc = InMemoryBucketClients.fresh();
        assertThrows(IllegalArgumentException.class,
                () -> new EventsOnMulticloudj(bc, "", 60));
    }

    @Test
    public void testGet_limitCapsResultSize() throws IOException {
        final Events events = cantor.events();
        final String ns = "limit-test-" + UUID.randomUUID();
        events.create(ns);

        long base = System.currentTimeMillis();
        // Store 5 events at consecutive timestamps
        for (int i = 0; i < 5; i++) {
            events.store(ns, base + i, Collections.singletonMap("i", String.valueOf(i)), null);
        }
        eventsImpl.forceFlushNow();

        List<Events.Event> capped = events.get(ns, base, base + 5, null, null, false, true, 3);
        assertEquals(capped.size(), 3, "limit=3 should cap result set to 3 events");

        events.drop(ns);
    }

    @Test
    public void testGet_descendingOrder() throws IOException {
        final Events events = cantor.events();
        final String ns = "descending-test-" + UUID.randomUUID();
        events.create(ns);

        long base = System.currentTimeMillis();
        for (int i = 0; i < 3; i++) {
            events.store(ns, base + i, Collections.singletonMap("i", String.valueOf(i)), null);
        }
        eventsImpl.forceFlushNow();

        List<Events.Event> descending = events.get(ns, base, base + 3, null, null, false, false, 0);
        assertEquals(descending.size(), 3);
        // Descending: newest first
        assertTrue(descending.get(0).getTimestampMillis() >= descending.get(1).getTimestampMillis(),
                "First result should have the largest timestamp");
        assertTrue(descending.get(1).getTimestampMillis() >= descending.get(2).getTimestampMillis(),
                "Results should be in descending order");

        events.drop(ns);
    }

    @Test
    public void testExpire_removesOldEvents() throws IOException {
        final Events events = cantor.events();
        final String ns = "expire-test-" + UUID.randomUUID();
        events.create(ns);

        long now = System.currentTimeMillis();
        long old = now - TimeUnit.HOURS.toMillis(2);

        events.store(ns, old, Collections.singletonMap("age", "old"), null);
        events.store(ns, now, Collections.singletonMap("age", "new"), null);
        eventsImpl.forceFlushNow();

        events.expire(ns, now - TimeUnit.HOURS.toMillis(1));

        List<Events.Event> remaining = events.get(ns, old, now + 1, null, null);
        assertEquals(remaining.size(), 1);
        assertEquals(remaining.get(0).getMetadata().get("age"), "new");

        events.drop(ns);
    }

    // ─── F2: restrictive buffer-directory permissions (CWE-276/200) ──────────────

    @Test
    public void testBufferDirectory_createdWithPosix700Permissions() throws IOException {
        // Skip on non-POSIX filesystems (e.g. Windows). On macOS/Linux, verify 700.
        if (!FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
            return;
        }

        // Create a parent that is deliberately *broader* than 700 (755) so we can prove
        // the constructor explicitly tightens the per-instance buffer root regardless of
        // the parent's mode.
        Path parent = Files.createTempDirectory("cantor-events-f2-parent-");
        Files.setPosixFilePermissions(parent, PosixFilePermissions.fromString("rwxr-xr-x"));

        BucketClient bc = InMemoryBucketClients.fresh();
        try (EventsOnMulticloudj events = new EventsOnMulticloudj(bc, parent.toString(), 60)) {
            // The constructor creates exactly one per-instance subdirectory under `parent`
            // named with the instance UUID; assert that subdir has 700 perms.
            try (java.util.stream.Stream<Path> children = Files.list(parent)) {
                List<Path> instanceDirs = children.filter(Files::isDirectory)
                        .collect(java.util.stream.Collectors.toList());
                assertEquals(instanceDirs.size(), 1, "Expected exactly one per-instance buffer subdir");
                Set<PosixFilePermission> perms = Files.getPosixFilePermissions(instanceDirs.get(0));
                Set<PosixFilePermission> expected = new HashSet<>(Arrays.asList(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE,
                        PosixFilePermission.OWNER_EXECUTE));
                assertEquals(perms, expected,
                        "Per-instance buffer subdir should be created with 700 permissions on POSIX systems");
            }
        }
    }

    // ─── F4: expire() must flushBeforeRead so buffered events get expired ────────

    @Test
    public void testExpire_flushesBufferedEventsBeforeExpiry() throws IOException {
        // R-D: prior to the fix, expire() didn't call flushBeforeRead(), so events still
        // sitting in the in-memory buffer survived the expiry call and reappeared on later
        // reads. Use a long flush interval so the scheduled flusher is guaranteed NOT to
        // run during the test — the only way these events make it to storage (and are then
        // subject to deletion by expire()) is if expire() itself flushes first.
        BucketClient bc = InMemoryBucketClients.fresh();
        try (EventsOnMulticloudj events = new EventsOnMulticloudj(bc, createTempBufferDir(), 3600)) {
            String ns = "f4-expire-flushes-" + UUID.randomUUID();
            events.create(ns);

            long now = System.currentTimeMillis();
            long old = now - TimeUnit.HOURS.toMillis(2);

            // Two events in the past, still sitting in the buffer (no forceFlushNow).
            events.store(ns, old, Collections.singletonMap("age", "old-1"), null);
            events.store(ns, old + 1, Collections.singletonMap("age", "old-2"), null);

            // expire everything older than 1 hour ago - must flush first, then delete.
            events.expire(ns, now - TimeUnit.HOURS.toMillis(1));

            List<Events.Event> remaining = events.get(ns, old, now + 1, null, null);
            assertEquals(remaining.size(), 0,
                    "Buffered events within expiry window should be flushed and then deleted by expire()");

            events.drop(ns);
        }
    }

    // ─── F3: validate and canonicalize buffer directory path (CWE-22) ────────────

    @Test
    public void testConstructor_rejectsDotDotInBufferPath() {
        BucketClient bc = InMemoryBucketClients.fresh();
        assertThrows(IllegalArgumentException.class,
                () -> new EventsOnMulticloudj(bc, "../../etc/cantor-buf", 60));
    }

    @Test
    public void testConstructor_rejectsDotDotMiddleSegment() {
        BucketClient bc = InMemoryBucketClients.fresh();
        assertThrows(IllegalArgumentException.class,
                () -> new EventsOnMulticloudj(bc, "/tmp/../etc/cantor-buf", 60));
    }

    @Test
    public void testConstructor_rejectsExistingNonDirectoryPath() throws IOException {
        // Create a regular file and pass it as buffer "dir" — should be rejected.
        Path tmpFile = Files.createTempFile("cantor-events-multicloudj-test-not-a-dir", ".txt");
        try {
            BucketClient bc = InMemoryBucketClients.fresh();
            assertThrows(IllegalArgumentException.class,
                    () -> new EventsOnMulticloudj(bc, tmpFile.toString(), 60));
        } finally {
            Files.deleteIfExists(tmpFile);
        }
    }

    @Test
    public void testConstructor_acceptsAbsoluteCleanPath() throws IOException {
        Path bufferDir = createTempBufferDirPath();
        BucketClient bc = InMemoryBucketClients.fresh();
        try (EventsOnMulticloudj events = new EventsOnMulticloudj(bc, bufferDir.toString(), 60)) {
            // Constructor must succeed for a clean absolute path.
            assertTrue(Files.isDirectory(bufferDir));
        }
    }

    private static String createTempBufferDir() {
        return createTempBufferDirPath().toString();
    }

    private static Path createTempBufferDirPath() {
        try {
            return Files.createTempDirectory("cantor-events-multicloudj-test-");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final class TestCantor implements Cantor {
        private final EventsOnMulticloudj events;
        private final ObjectsOnMulticloudj objects;

        TestCantor(BucketClient bucketClient, EventsOnMulticloudj events) throws IOException {
            this.events = events;
            this.objects = new ObjectsOnMulticloudj(bucketClient);
        }

        @Override
        public Objects objects() {
            return objects;
        }

        @Override
        public Sets sets() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Events events() {
            return events;
        }
    }
}
