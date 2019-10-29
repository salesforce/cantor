package com.salesforce.cantor.archive;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.Sets;
import com.salesforce.cantor.h2.CantorOnH2;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsDeep;
import static org.testng.Assert.assertNotEquals;
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
        CantorArchiver.archive(cantor.objects(), namespace, outputPath);

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
        CantorArchiver.archive(cantor.objects(), namespace, outputPath);
        assertTrue(Files.exists(outputPath), "archiving zero objects should still produce file");

        CantorArchiver.restore(cantor.objects(), namespace, outputPath);
        assertEquals(cantor.objects().size(namespace), 0,  "shouldn't have restored any objects");
    }

    @Test
    public void testArchiveSets() throws IOException {
        final String basePath = "/tmp/cantor-archive-objects-test/" + UUID.randomUUID().toString();
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
        CantorArchiver.archive(cantor.sets(), namespace, outputPath);

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

    private static Cantor getCantor(final String path) throws IOException {
        return new CantorOnH2(path);
    }

}