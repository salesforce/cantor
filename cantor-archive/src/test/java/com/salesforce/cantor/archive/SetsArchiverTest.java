/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Sets;
import com.salesforce.cantor.h2.CantorOnH2;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.testng.Assert.*;

public class SetsArchiverTest {

    @Test
    public void testArchiveSets() throws IOException {
        final String basePath = Paths.get(System.getProperty("java.io.tmpdir"), "cantor-archive-sets-test", UUID.randomUUID().toString()).toString();
        final Cantor cantor = getCantor(Paths.get(basePath, "input").toString());
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
        SetsArchiver.archive(cantor.sets(), namespace, outputPath, SetsArchiver.MAX_CHUNK_SIZE);

        assertTrue(Files.exists(outputPath), "archive file missing");
        assertNotEquals(Files.size(outputPath), 0, "empty archive file shouldn't exist");

        final String vNamespace = UUID.randomUUID().toString();
        final Cantor vCantor = getCantor(Paths.get(basePath, "verify").toString());
        // sanity check
        for (final String set : allSets.keySet()) {
            assertThrows(IOException.class, () -> vCantor.sets().size(vNamespace, set));
        }

        SetsArchiver.restore(vCantor.sets(), vNamespace, outputPath);
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

    private static Cantor getCantor(final String path) throws IOException {
        return new CantorOnH2(path);
    }
}