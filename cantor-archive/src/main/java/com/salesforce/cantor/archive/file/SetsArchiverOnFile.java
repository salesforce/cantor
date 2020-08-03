/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive.file;

import com.salesforce.cantor.Sets;
import com.salesforce.cantor.archive.SetsChunk;
import com.salesforce.cantor.misc.archivable.SetsArchiver;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.common.CommonPreconditions.checkString;

public class SetsArchiverOnFile extends AbstractBaseArchiverOnFile implements SetsArchiver {
    private static final Logger logger = LoggerFactory.getLogger(SetsArchiverOnFile.class);
    private static final String archivePathFormat = "/archive-sets-%s";
    private static final Pattern archiveRegexPattern = Pattern.compile("archive-sets-(?<namespace>.*)");

    public static final int chunkSize = 1_000;

    public SetsArchiverOnFile(final String baseDirectory) {
        super(baseDirectory);
        setSubDirectory("sets");
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        return Files.list(getArchiveLocation())
                .map(SetsArchiverOnFile::getNamespace)
                .collect(Collectors.toList());
    }

    @Override
    public void create(final String namespace) throws IOException {
        // no-op; creating an archive only happens during deletion of hot data
    }

    @Override
    public void drop(final String namespace) throws IOException {
        final Path archiveFile = getFileArchive(namespace);
        checkArgument(archiveFile.toFile().exists(), "file archive does not exist: " + archiveFile);
        if (!archiveFile.toFile().delete()) {
            throw new IOException("failed to delete namespace archive: " + archiveFile.toString());
        }
    }

    @Override
    public void archive(final Sets sets, final String namespace) throws IOException {
        final Path destination = getFileArchive(namespace);
        checkArchiveArguments(sets, namespace, destination);
        doArchive(sets, namespace, destination);
    }

    @Override
    public void restore(final Sets sets, final String namespace) throws IOException {
        final Path archiveFile = getFileArchive(namespace);
        checkRestoreArguments(sets, namespace, archiveFile);
        doRestore(sets, namespace, archiveFile);
    }

    protected void doArchive(final Sets sets, final String namespace, final Path destination) throws IOException {
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

    protected void doRestore(final Sets sets, final String namespace, final Path archiveFile) throws IOException {
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

    protected Path getFileArchive(final String namespace) {
        checkString(namespace, "null/empty namespace");
        return getFile(archivePathFormat, namespace);
    }

    // extracts namespace from a filename
    protected static String getNamespace(final Path path) {
        final String fileName = path.getFileName().toString();
        final Matcher matcher = archiveRegexPattern.matcher(fileName);
        return (matcher.matches()) ? matcher.group("namespace") : null;
    }
}
