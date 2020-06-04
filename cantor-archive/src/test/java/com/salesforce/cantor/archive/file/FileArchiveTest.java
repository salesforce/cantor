/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive.file;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.archive.EventsChunk;
import com.salesforce.cantor.h2.CantorOnH2;
import com.salesforce.cantor.misc.archivable.ArchivableCantor;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class FileArchiveTest {
    private static final Map<String, Long> CANTOR_H2_NAMESPACES = new HashMap<>();
    private static final long TIMEFRAME_BOUND = System.currentTimeMillis();
    private static final long TIMEFRAME_ORIGIN = TIMEFRAME_BOUND - TimeUnit.DAYS.toMillis(2);
    private static final String H2_DIRECTORY = "/tmp/cantor-s3-on-local";
    private static final String BASE_DIRECTORY = "/tmp/cantor-archive-test";
    private static final long HOUR_MILLIS = TimeUnit.HOURS.toMillis(1);

    private Cantor localCantor;
    private FileArchiver archiver;

    @BeforeMethod
    public void setup() throws IOException {
        final File baseDirectory = new File(BASE_DIRECTORY);
        baseDirectory.delete();
        baseDirectory.mkdirs();
        this.archiver = new FileArchiver(BASE_DIRECTORY, 100, HOUR_MILLIS);
        this.localCantor = new ArchivableCantor(new CantorOnH2(H2_DIRECTORY), archiver);
        generateData();
    }

    @AfterMethod
    public void teardown() {
        final File baseDirectory = new File(BASE_DIRECTORY);
        final String[] entries = baseDirectory.list();
        if (entries != null) {
            for(final String file : entries) {
                final File currentFile = new File(baseDirectory.getPath(), file);
                currentFile.delete();
            }
        }
        baseDirectory.delete();
    }

    @Test
    public void testEventsArchive() throws IOException {
        for (final Map.Entry<String, Long> cantorH2Namespace : CANTOR_H2_NAMESPACES.entrySet()) {
            final long endTimestamp = getFloorForWindow(cantorH2Namespace.getValue(), HOUR_MILLIS) - 1;
            final List<Events.Event> events = this.localCantor.events()
                    .get(cantorH2Namespace.getKey(), TIMEFRAME_ORIGIN, endTimestamp + HOUR_MILLIS);
            this.localCantor.events().expire(cantorH2Namespace.getKey(), cantorH2Namespace.getValue());
            validateArchive(events, cantorH2Namespace.getKey(), endTimestamp);
        }
    }

    @Test
    public void testEventsArchiveIdempotent() throws IOException {
        for (final Map.Entry<String, Long> cantorH2Namespace : CANTOR_H2_NAMESPACES.entrySet()) {
            final long endTimestamp = getFloorForWindow(cantorH2Namespace.getValue(), HOUR_MILLIS) - 1;
            final List<Events.Event> events = this.localCantor.events()
                    .get(cantorH2Namespace.getKey(), TIMEFRAME_ORIGIN, endTimestamp + HOUR_MILLIS);
            this.localCantor.events().expire(cantorH2Namespace.getKey(), cantorH2Namespace.getValue());
            validateArchive(events, cantorH2Namespace.getKey(), endTimestamp);
            // run again
            final List<Events.Event> noEvents = this.localCantor.events()
                    .get(cantorH2Namespace.getKey(), TIMEFRAME_ORIGIN, endTimestamp + HOUR_MILLIS);
            Assert.assertEquals(noEvents.size(), 0, "all events should have been archived on last run");
            this.localCantor.events().expire(cantorH2Namespace.getKey(), cantorH2Namespace.getValue());
            // intentionally checking that noEvents had no impact by validating with events
            validateArchive(events, cantorH2Namespace.getKey(), endTimestamp);
            // run again and expire new events
            final List<Events.Event> moreEvents = this.localCantor.events()
                    .get(cantorH2Namespace.getKey(), TIMEFRAME_ORIGIN, TIMEFRAME_BOUND);
            this.localCantor.events().expire(cantorH2Namespace.getKey(), TIMEFRAME_BOUND);
            validateArchive(moreEvents, cantorH2Namespace.getKey(), TIMEFRAME_BOUND);
        }
    }

    @Test
    public void testEventsArchiveZero() throws IOException {
        for (final String cantorH2Namespace : CANTOR_H2_NAMESPACES.keySet()) {
            final List<Events.Event> events = this.localCantor.events()
                    .get(cantorH2Namespace, 0, HOUR_MILLIS);
            this.localCantor.events().expire(cantorH2Namespace, HOUR_MILLIS);
            validateArchive(events, cantorH2Namespace, HOUR_MILLIS);
        }
    }

    private void validateArchive(final List<Events.Event> events,
                                 final String cantorH2Namespace,
                                 final long endTimestamp) throws IOException {
        if (events.size() > 0) {
            int eventCount = 0;
            final FileEventsArchiver fileEventsArchiver = (FileEventsArchiver) this.archiver.eventsArchiver();
            for (long end = endTimestamp; end > 0; end -= HOUR_MILLIS) {
                final Path fileArchive = fileEventsArchiver.getFileArchive(cantorH2Namespace, end + 1);
                if (!fileArchive.toFile().exists()) return;

                try (final ArchiveInputStream archiveInputStream = fileEventsArchiver.getArchiveInputStream(fileArchive)) {
                    while (archiveInputStream.getNextEntry() != null) {
                        final EventsChunk chunk = EventsChunk.parseFrom(archiveInputStream);
                        eventCount += chunk.getEventsCount();
                    }
                }
            }
            Assert.assertEquals(eventCount, events.size(), "events that were expired were not archived");
        } else {
            final String[] files = new File(BASE_DIRECTORY).list();
            if (files == null) return;

            final List<String> filesList = Arrays.asList(files);
            filesList.forEach(file -> Assert.assertFalse(file.contains(cantorH2Namespace), "no events archived but found archive file for namespace: " + cantorH2Namespace));
        }
    }

    private void generateData() throws IOException {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int namespaceCount = 0; namespaceCount < random.nextInt(2, 5); namespaceCount++) {
            final String namespace = "cantor-archive-test-" + Math.abs(UUID.randomUUID().hashCode());
            this.localCantor.events().create(namespace);
            CANTOR_H2_NAMESPACES.put(namespace, random.nextLong(TIMEFRAME_ORIGIN, TIMEFRAME_BOUND));

            for (int eventCount = 0; eventCount < random.nextInt(100, 1000); eventCount++) { // 1GB max
                final byte[] randomPayload = new byte[random.nextInt(0, 1_000_000)]; // 1MB max
                random.nextBytes(randomPayload);
                this.localCantor.events().store(
                    namespace, random.nextLong(TIMEFRAME_ORIGIN, TIMEFRAME_BOUND),
                        null,null, randomPayload
                );
            }

            for (int eventCount = 0; eventCount < random.nextInt(1, 10); eventCount++) { // 1MB max
                // throw in a few random events at zero timestamp
                final byte[] randomPayload = new byte[random.nextInt(0, 100_000)]; // 100KB max
                random.nextBytes(randomPayload);
                this.localCantor.events().store(
                        namespace, 0,
                        null,null, null
                );
            }
        }
    }

    private long getFloorForWindow(final long timestampMillis, final long chunkMillis) {
        return (timestampMillis / chunkMillis) * chunkMillis;
    }
}
