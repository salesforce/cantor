/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.Events.Event;
import com.salesforce.cantor.h2.CantorOnH2;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

public class EventsArchiverOnFileTest {
    @Test
    public void testArchiveEvents() throws IOException {
        final String basePath = Paths.get(System.getProperty("java.io.tmpdir"), "cantor-archive-sets-test", UUID.randomUUID().toString()).toString();
        final Cantor cantor = getCantor(Paths.get(basePath, "input").toString());
        final String namespace = UUID.randomUUID().toString();

        final long now = System.currentTimeMillis();
        final long standardWindowMillis = ThreadLocalRandom.current().nextLong(TimeUnit.HOURS.toMillis(1L), TimeUnit.HOURS.toMillis(2L));
        final long standardStart = now - standardWindowMillis;
        final Map<String, Event> stored = populateEvents(cantor.events(), namespace, standardStart, now, 1_000);
        // sanity check
        final List<Event> actual = cantor.events().get(namespace, standardStart, now, true);
        assertEquals(actual.size(), stored.size(), "didn't store expected amount of events");
        for (final Event actualEvent : actual) {
            assertTrue(actualEvent.getMetadata().containsKey("guid"), "event missing guid");
            final Event expectedEvent = stored.get(actualEvent.getMetadata().get("guid"));
            assertEventsEqual(actualEvent, expectedEvent);
        }

        // archive events in 1 minute chunks
        Files.createDirectories(Paths.get(basePath, "output"));
        final Path outputPath = Paths.get(basePath, "output",  "test-archive.tar.gz");
        EventsArchiverOnFile.archive(cantor.events(), namespace, standardStart, now, null, null, TimeUnit.MINUTES.toMillis(1L), outputPath);
        // check file was created and non-empty
        assertTrue(Files.exists(outputPath), "archive file missing");
        assertNotEquals(Files.size(outputPath), 0, "empty archive file shouldn't exist");

        // choose a new cantor/namespace to restore to
        final String vNamespace = UUID.randomUUID().toString();
        final Cantor vCantor = getCantor(Paths.get(basePath, "verify").toString());
        // sanity check
        assertThrows(IOException.class, () -> vCantor.events().get(namespace, standardStart, now, true));

        // restore events and check everything got restored
        EventsArchiverOnFile.restore(vCantor.events(), vNamespace, outputPath);
        final List<Event> restored = vCantor.events().get(vNamespace, standardStart, now, true);
        assertEquals(restored.size(), stored.size(), "didn't store expected amount of events");
        for (final Event restoredEvent : restored) {
            assertTrue(restoredEvent.getMetadata().containsKey("guid"), "event missing guid");
            final Event expectedEvent = stored.get(restoredEvent.getMetadata().get("guid"));
            assertEventsEqual(restoredEvent, expectedEvent);
        }

        assertTrue(Files.exists(outputPath), "archive file missing");
        assertNotEquals(Files.size(outputPath), 0, "empty archive file shouldn't exist");

        // reset verify namespace
        vCantor.events().drop(vNamespace);
        assertThrows(IOException.class, () -> vCantor.events().get(namespace, standardStart, now, true));
        // populate events that overlap with previous time window
        final long overlapStart = standardStart - ThreadLocalRandom.current().nextLong(TimeUnit.HOURS.toMillis(1L), TimeUnit.HOURS.toMillis(2L));
        final long overlapEnd = now - ThreadLocalRandom.current().nextLong(TimeUnit.MINUTES.toMillis(15L), TimeUnit.MINUTES.toMillis(30L));
        populateEvents(cantor.events(), namespace, overlapStart, overlapEnd, 1_000);
        final Map<String, Event> overlapStored = new HashMap<>();
        for (final Event event : cantor.events().get(namespace, overlapStart, overlapEnd, true)) {
            assertTrue(event.getMetadata().containsKey("guid"), "missing guid, can't verify");
            overlapStored.put(event.getMetadata().get("guid"), event);
        }

        // archive overlap
        final Path overlapOutputPath = Paths.get(basePath, "output",  "test-overlap-archive.tar.gz");
        EventsArchiverOnFile.archive(cantor.events(), namespace, overlapStart, overlapEnd, null, null, TimeUnit.MINUTES.toMillis(10L), overlapOutputPath);
        // restore overlap
        assertTrue(Files.exists(overlapOutputPath), "archive file missing");
        assertNotEquals(Files.size(overlapOutputPath), 0, "empty archive file shouldn't exist");
        EventsArchiverOnFile.restore(vCantor.events(), vNamespace, overlapOutputPath);

        final List<Event> overlapRestored = vCantor.events().get(vNamespace, overlapStart, overlapEnd, true);
        assertEquals(overlapRestored.size(), overlapStored.size(), "didn't store expected amount of events");
        for (final Event restoredEvent : overlapRestored) {
            assertTrue(restoredEvent.getMetadata().containsKey("guid"), "event missing guid");
            final Event expectedEvent = overlapStored.get(restoredEvent.getMetadata().get("guid"));
            assertEventsEqual(restoredEvent, expectedEvent);
        }

        // check empty namespace archive
        final String emptyNamespace = UUID.randomUUID().toString();
        cantor.events().create(emptyNamespace);
        // archive empty
        final Path emptyOutputPath = Paths.get(basePath, "output", "test-empty-archive.tar.gz");
        EventsArchiverOnFile.archive(cantor.events(), emptyNamespace, standardStart, now, null, null,  60_000L, emptyOutputPath);
        assertTrue(Files.exists(emptyOutputPath), "archiving zero events should still produce file");
        // verify
        final String emptyVerificationNamespace = UUID.randomUUID().toString();
        EventsArchiverOnFile.restore(vCantor.events(), emptyVerificationNamespace, emptyOutputPath);
        assertTrue(vCantor.events().get(emptyVerificationNamespace, standardStart, now).isEmpty(), "should be no events");

        // check queries
        final String queryNamespace = UUID.randomUUID().toString();
        cantor.events().create(queryNamespace);
        final Map<String, Event> queryStored = populateEvents(cantor.events(), queryNamespace, standardStart, now, 1_000);

        final Path queryOutputPath = Paths.get(basePath, "output", "test-query-archive.tar.gz");
        final Double dimTarget = ThreadLocalRandom.current().nextBoolean() ? 1D : 0D;
        final Map<String, String> dims = Collections.singletonMap("dim-check", dimTarget.toString());
        final String metaTarget = String.valueOf(ThreadLocalRandom.current().nextBoolean());
        final Map<String, String> meta = Collections.singletonMap("meta-check", metaTarget);
        EventsArchiverOnFile.archive(cantor.events(), queryNamespace, standardStart, now, meta, dims,  60_000L, queryOutputPath);
        assertTrue(Files.exists(queryOutputPath), "archiving events with queries should still produce file");

        // verify
        final String queryVerificationNamespace = UUID.randomUUID().toString();
        EventsArchiverOnFile.restore(vCantor.events(), queryVerificationNamespace, queryOutputPath);
        final List<Event> queryEvents = vCantor.events().get(queryVerificationNamespace, standardStart, now, true);
        for (final Event event : queryEvents) {
            assertEquals(event.getDimensions().get("dim-check"), dimTarget);
            assertEquals(event.getMetadata().get("meta-check"), metaTarget);
            assertEventsEqual(event, queryStored.get(event.getMetadata().get("guid")));
        }
    }

    private void assertEventsEqual(final Event actual, final Event expected) {
        assertEquals(actual.getTimestampMillis(), expected.getTimestampMillis(), "timestamps don't match");
        assertEqualsDeep(actual.getDimensions(), expected.getDimensions(), "dimensions don't match");
        assertEqualsDeep(actual.getMetadata(), expected.getMetadata(), "metadata don't match");
        if (expected.getPayload() == null) {
            assertTrue(actual.getPayload() ==  null || actual.getPayload().length == 0,
                    "expected null/empty payload, got:" + Arrays.toString(actual.getPayload()));
        } else {
            assertEquals(actual.getPayload(), expected.getPayload(), "payloads don't match");
        }
    }

    private static Map<String, Event> populateEvents(final Events events, final String namespace, final long start,  final long end, final int count) throws IOException {
        events.create(namespace);
        final Map<String, Event> stored = new HashMap<>();
        for (int i = 0; i < count; i++) {
            final Event event;
            if (ThreadLocalRandom.current().nextBoolean()) {
                event = new Event(ts(start, end), meta(), dim(), UUID.randomUUID().toString().getBytes());
            } else {
                event = new Event(ts(start, end), meta(), dim());
            }
            assertNull(event.getMetadata().put("guid", UUID.randomUUID().toString()), "already stored this guid");
            events.store(namespace, event);
            stored.put(event.getMetadata().get("guid"), event);
        }
        return stored;
    }

    private static long ts(final long start, final long end) {
        return ThreadLocalRandom.current().nextLong(start, end);
    }

    private static Map<String, String> meta() {
        final Map<String, String> meta = new HashMap<>();
        for (int i = 0; i < ThreadLocalRandom.current().nextInt(0, 10); i++) {
            meta.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            meta.put("meta-check", String.valueOf(ThreadLocalRandom.current().nextBoolean()));
        }
        return meta;
    }

    private static Map<String, Double> dim() {
        final Map<String, Double> dim = new HashMap<>();
        for (int i = 0; i < ThreadLocalRandom.current().nextInt(0, 10); i++) {
            dim.put(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble());
            dim.put("dim-check", ThreadLocalRandom.current().nextBoolean() ? 1D : 0D);
        }
        return dim;
    }

    private static Cantor getCantor(final String path) throws IOException {
        return new CantorOnH2(path);
    }
}