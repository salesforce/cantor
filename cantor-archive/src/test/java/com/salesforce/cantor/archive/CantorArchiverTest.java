package com.salesforce.cantor.archive;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.Events.Event;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.Sets;
import com.salesforce.cantor.h2.CantorOnH2;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsDeep;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class CantorArchiverTest {

    @Test
    public void testArchiveRestoreObjects() throws IOException {
        final String basePath = "/tmp/cantor-archive-objects-test/" + UUID.randomUUID().toString();
        final Cantor cantor = getCantor(basePath  + "/input/");
        final String namespace = UUID.randomUUID().toString();
        final Map<String, byte[]> stored = populateObjects(cantor.objects(), namespace, ThreadLocalRandom.current().nextInt(1_000, 5_000));
        assertEquals(cantor.objects().size(namespace), stored.size(), "didn't store expected values");

        Files.createDirectories(Paths.get(basePath, "output"));
        final Path outputPath = Paths.get(basePath, "output",  "test-archive.tar.gz");
        CantorArchiver.archive(cantor.objects(), namespace, outputPath, CantorArchiver.MAX_OBJECT_CHUNK_SIZE);

        assertTrue(Files.exists(outputPath), "archive file missing");
        assertNotEquals(Files.size(outputPath), 0, "empty archive file shouldn't exist");

        final String vNamespace = UUID.randomUUID().toString();
        final Cantor vCantor = getCantor(basePath + "/verify/");
        assertThrows(IOException.class, () -> vCantor.objects().size(vNamespace));
        CantorArchiver.restore(vCantor.objects(), vNamespace, outputPath);

        assertEquals(vCantor.objects().size(vNamespace), stored.size(), "didn't restore expected number of objects");
        final Collection<String> vKeys = vCantor.objects().keys(vNamespace, 0, -1);
        for (final String key : vKeys) {
            assertEquals(vCantor.objects().get(vNamespace, key), stored.get(key), "restored object doesn't match stored");
        }
    }

    @Test
    public void testArchiveZeroObjectsNamespace() throws IOException {
        final String basePath = "/tmp/cantor-archive-objects-test-zero/" + UUID.randomUUID().toString();
        final Cantor cantor = getCantor(basePath  + "/input/");
        final String namespace = UUID.randomUUID().toString();
        cantor.objects().create(namespace);

        Files.createDirectories(Paths.get(basePath, "output"));
        final Path outputPath = Paths.get(basePath, "output", "test-archive.tar.gz");
        CantorArchiver.archive(cantor.objects(), namespace, outputPath, CantorArchiver.MAX_SETS_CHUNK_SIZE);
        assertTrue(Files.exists(outputPath), "archiving zero objects should still produce file");

        CantorArchiver.restore(cantor.objects(), namespace, outputPath);
        assertEquals(cantor.objects().size(namespace), 0,  "shouldn't have restored any objects");
    }

    @Test
    public void testArchiveSets() throws IOException {
        final String basePath = "/tmp/cantor-archive-sets-test/" + UUID.randomUUID().toString();
        final Cantor cantor = getCantor(basePath  + "/input/");
        final String namespace = UUID.randomUUID().toString();

        final Map<String, Map<String, Long>> allSets = new HashMap<>();
        for (int i = 0; i < ThreadLocalRandom.current().nextInt(5, 10); i++) {
            final String set = UUID.randomUUID().toString();
            allSets.put(set, populateSet(cantor.sets(), namespace, set, ThreadLocalRandom.current().nextInt(1_000, 5_000)));
        }
        // sanity check
        for (final String set : allSets.keySet()) {
            assertEquals(cantor.sets().size(namespace, set), allSets.get(set).size(), "didn't store expected count for set");
        }

        Files.createDirectories(Paths.get(basePath, "output"));
        final Path outputPath = Paths.get(basePath, "output",  "test-archive.tar.gz");
        CantorArchiver.archive(cantor.sets(), namespace, outputPath, CantorArchiver.MAX_SETS_CHUNK_SIZE);

        assertTrue(Files.exists(outputPath), "archive file missing");
        assertNotEquals(Files.size(outputPath), 0, "empty archive file shouldn't exist");

        final String vNamespace = UUID.randomUUID().toString();
        final Cantor vCantor = getCantor(basePath + "/verify/");
        // sanity check
        for (final String set : allSets.keySet()) {
            assertThrows(IOException.class, () -> vCantor.sets().size(vNamespace, set));
        }

        CantorArchiver.restore(vCantor.sets(), vNamespace, outputPath);
        for (final String set : allSets.keySet()) {
            final Map<String, Long> expectedEntries = allSets.get(set);
            assertEquals(vCantor.sets().size(vNamespace, set), expectedEntries.size(), "didn't restore expected number of entries");

            final Map<String, Long> actualEntries = vCantor.sets().get(vNamespace, set);
            assertEqualsDeep(actualEntries, expectedEntries, "restored entries don't match expected");
        }
    }

    @Test
    public void testArchiveEvents() throws IOException {
        final String basePath = "/tmp/cantor-archive-events-test/" + UUID.randomUUID().toString();
        final Cantor cantor = getCantor(basePath  + "/input/");
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
        CantorArchiver.archive(cantor.events(), namespace, standardStart, now, TimeUnit.MINUTES.toMillis(1L), outputPath);
        // check file was created and non-empty
        assertTrue(Files.exists(outputPath), "archive file missing");
        assertNotEquals(Files.size(outputPath), 0, "empty archive file shouldn't exist");

        // choose a new cantor/namespace to restore to
        final String vNamespace = UUID.randomUUID().toString();
        final Cantor vCantor = getCantor(basePath + "/verify/");
        // sanity check
        assertThrows(IOException.class, () -> vCantor.events().get(namespace, standardStart, now, true));

        // restore events and check everything got restored
        CantorArchiver.restore(vCantor.events(), vNamespace, outputPath);
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
        final Map<String, Events.Event> overlapStored = new HashMap<>();
        for (final Event event : cantor.events().get(namespace, overlapStart, overlapEnd, true)) {
            assertTrue(event.getMetadata().containsKey("guid"), "missing guid, can't verify");
            overlapStored.put(event.getMetadata().get("guid"), event);
        }

        // archive overlap
        final Path overlapOutputPath = Paths.get(basePath, "output",  "test-overlap-archive.tar.gz");
        CantorArchiver.archive(cantor.events(), namespace, overlapStart, overlapEnd, TimeUnit.MINUTES.toMillis(10L), overlapOutputPath);
        // restore overlap
        assertTrue(Files.exists(overlapOutputPath), "archive file missing");
        assertNotEquals(Files.size(overlapOutputPath), 0, "empty archive file shouldn't exist");
        CantorArchiver.restore(vCantor.events(), vNamespace, overlapOutputPath);

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
        CantorArchiver.archive(cantor.events(), emptyNamespace, standardStart, now,  60_000L, emptyOutputPath);
        assertTrue(Files.exists(emptyOutputPath), "archiving zero events should still produce file");
        // verify
        final String emptyVerificationNamespace = UUID.randomUUID().toString();
        CantorArchiver.restore(vCantor.events(), emptyVerificationNamespace, emptyOutputPath);
        assertTrue(vCantor.events().get(emptyVerificationNamespace, standardStart, now).isEmpty(), "should be no events");
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

    private static Map<String, Long> populateSet(final Sets sets, final String namespace, final String set, final int count) throws IOException {
        sets.create(namespace);
        final Map<String, Long> stored = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            final String entry = UUID.randomUUID().toString();
            final long weight = ThreadLocalRandom.current().nextLong();
            sets.add(namespace, set, entry, weight);
            stored.put(entry, weight);
        }
        return stored;
    }

    private static Map<String, byte[]> populateObjects(final Objects objects, final String namespace, final int count) throws IOException {
        objects.create(namespace);
        final Map<String, byte[]> stored = new HashMap<>();
        for (int i = 0; i < count; i++) {
            final String key = UUID.randomUUID().toString();
            stored.put(key, key.getBytes());
            objects.store(namespace, key, key.getBytes());
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
        }
        return meta;
    }

    private static Map<String, Double> dim() {
        final Map<String, Double> dim = new HashMap<>();
        for (int i = 0; i < ThreadLocalRandom.current().nextInt(0, 10); i++) {
            dim.put(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble());
        }
        return dim;
    }

    private static Cantor getCantor(final String path) throws IOException {
        return new CantorOnH2(path);
    }

}