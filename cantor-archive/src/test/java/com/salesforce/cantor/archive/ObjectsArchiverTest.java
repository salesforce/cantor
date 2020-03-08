/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.h2.CantorOnH2;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.testng.Assert.*;

public class ObjectsArchiverTest {

    @Test
    public void testArchiveRestoreObjects() throws IOException {
        final String basePath = Paths.get(System.getProperty("java.io.tmpdir"), "cantor-archive-objects-test", UUID.randomUUID().toString()).toString();
        final Cantor cantor = getCantor(Paths.get(basePath, "input").toString());
        final String namespace = UUID.randomUUID().toString();
        final Map<String, byte[]> stored = populateObjects(cantor.objects(), namespace, ThreadLocalRandom.current().nextInt(1_000, 5_000));
        assertEquals(cantor.objects().size(namespace), stored.size(), "didn't store expected values");

        Files.createDirectories(Paths.get(basePath, "output"));
        final Path outputPath = Paths.get(basePath, "output",  "test-archive.tar.gz");
        ObjectsArchiver.archive(cantor.objects(), namespace, outputPath, ObjectsArchiver.MAX_CHUNK_SIZE);

        assertTrue(Files.exists(outputPath), "archive file missing");
        assertNotEquals(Files.size(outputPath), 0, "empty archive file shouldn't exist");

        final String vNamespace = UUID.randomUUID().toString();
        final Cantor vCantor = getCantor(Paths.get(basePath, "verify").toString());
        assertThrows(IOException.class, () -> vCantor.objects().size(vNamespace));
        ObjectsArchiver.restore(vCantor.objects(), vNamespace, outputPath);

        assertEquals(vCantor.objects().size(vNamespace), stored.size(), "didn't restore expected number of objects");
        final Collection<String> vKeys = vCantor.objects().keys(vNamespace, 0, -1);
        for (final String key : vKeys) {
            assertEquals(vCantor.objects().get(vNamespace, key), stored.get(key), "restored object doesn't match stored");
        }
    }

    @Test
    public void testArchiveZeroObjectsNamespace() throws IOException {
        final String basePath = Paths.get(System.getProperty("java.io.tmpdir"), "cantor-archive-zero-objects-test", UUID.randomUUID().toString()).toString();
        final Cantor cantor = getCantor(Paths.get(basePath, "input").toString());
        final String namespace = UUID.randomUUID().toString();
        cantor.objects().create(namespace);

        Files.createDirectories(Paths.get(basePath, "output"));
        final Path outputPath = Paths.get(basePath, "output", "test-archive.tar.gz");
        ObjectsArchiver.archive(cantor.objects(), namespace, outputPath, ObjectsArchiver.MAX_CHUNK_SIZE);
        assertTrue(Files.exists(outputPath), "archiving zero objects should still produce file");

        ObjectsArchiver.restore(cantor.objects(), namespace, outputPath);
        assertEquals(cantor.objects().size(namespace), 0,  "shouldn't have restored any objects");
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