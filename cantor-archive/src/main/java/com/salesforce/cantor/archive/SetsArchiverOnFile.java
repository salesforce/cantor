/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive;

import com.salesforce.cantor.Sets;
import com.salesforce.cantor.misc.archivable.SetsArchiver;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

public class SetsArchiverOnFile extends AbstractBaseArchiver implements SetsArchiver {
    private static final Logger logger = LoggerFactory.getLogger(SetsArchiverOnFile.class);

    public static final int MAX_CHUNK_SIZE = 1_000;

    public static void archive(final Sets sets, final String namespace, final Path destination, final int chunkSize) throws IOException {
        checkArchiveArguments(sets, namespace, destination);
        checkArgument(chunkSize <= MAX_CHUNK_SIZE, "chunk size must be <=" + MAX_CHUNK_SIZE);
        // get all sets for the namespace, any sets added after won't be archived
        final Collection<String> setNames = sets.sets(namespace);
        try (final ArchiveOutputStream archive = getArchiveOutputStream(destination)) {
            // archive each set one at a time
            for (final String set : setNames) {
                logger.info("archiving set {}.{}", namespace, set);
                int start = 0;
                Map<String, Long> entries = sets.get(namespace, set, start, chunkSize);
                while (!entries.isEmpty()) {
                    final int end = start + entries.size();
                    final String name = String.format("sets-%s-%s-%s-%s", namespace, set, start, end);
                    // store chunks as tar archives so we can restore them in chunks too
                    final SetsChunk chunk = SetsChunk.newBuilder().setSet(set).putAllEntries(entries).build();
                    writeArchiveEntry(archive, name, chunk.toByteArray());
                    logger.info("archived {} entries ({}-{}) into chunk '{}' for set {}.{}", entries.size(), start, end, name, namespace, set);
                    start = end;
                    entries = sets.get(namespace, set, start, chunkSize);
                }
            }
        }
    }

    public static void restore(final Sets sets, final String namespace, final Path archiveFile) throws IOException {
        checkRestoreArguments(sets, namespace, archiveFile);
        // create the namespace, in case the user hasn't already
        sets.create(namespace);
        try (final ArchiveInputStream archive = getArchiveInputStream(archiveFile)) {
            ArchiveEntry entry;
            int total = 0;
            while ((entry = archive.getNextEntry()) != null) {
                final SetsChunk chunk = SetsChunk.parseFrom(archive);
                sets.add(namespace, chunk.getSet(), chunk.getEntriesMap());
                logger.info("read {} entries from chunk {} ({} bytes) into {}.{}", chunk.getEntriesCount(), entry.getName(), entry.getSize(), namespace, chunk.getSet());
                total += chunk.getEntriesCount();
            }
            logger.info("restored {} entries int namespace '{}' from archive file {}", total, namespace, archiveFile);
        }
    }

    @Override
    public void archive(final Sets sets, final String namespace) throws IOException {

    }

    @Override
    public void restore(final Sets sets, final String namespace) throws IOException {

    }
}
