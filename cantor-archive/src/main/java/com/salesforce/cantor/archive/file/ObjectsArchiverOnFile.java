/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive.file;

import com.google.protobuf.ByteString;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.archive.ObjectsChunk;
import com.salesforce.cantor.misc.archivable.ObjectsArchiver;
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

public class ObjectsArchiverOnFile extends AbstractBaseArchiverOnFile implements ObjectsArchiver {
    private static final Logger logger = LoggerFactory.getLogger(ObjectsArchiverOnFile.class);
    protected static final String archivePathFormat = "/archive-objects-%s";
    private static final Pattern archiveRegexPattern = Pattern.compile("archive-objects-(?<namespace>.*)");

    public static final int chunkSize = 1_000;

    public ObjectsArchiverOnFile(final String baseDirectory) {
        super(baseDirectory);
        setSubDirectory("objects");
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        return Files.list(getArchiveLocation())
                .map(ObjectsArchiverOnFile::getNamespace)
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
    public void archive(final Objects objects, final String namespace) throws IOException {
        final Path destination = getFileArchive(namespace);
        checkArchiveArguments(objects, namespace, destination);
        doArchive(objects, namespace, destination);
    }

    @Override
    public void restore(final Objects objects, final String namespace) throws IOException {
        final Path archiveFile = getFileArchive(namespace);
        checkRestoreArguments(objects, namespace, archiveFile);
        doRestore(objects, namespace, archiveFile);
    }

    protected void doArchive(final Objects objects, final String namespace, final Path destination) throws IOException {
        try (final ArchiveOutputStream archive = getArchiveOutputStream(destination)) {
            // get objects to archive in chunks in case of large namespaces
            int start = 0;
            Collection<String> keys = objects.keys(namespace, start, chunkSize);
            while (!keys.isEmpty()) {
                final Map<String, byte[]> chunk = objects.get(namespace, keys);
                final int end = start + chunk.size();
                final String name = String.format("objects-%s-%s-%s", namespace, start, end);
                // store chunks as tar archives so we can restore them in chunks too
                writeArchiveEntry(archive, name, getBytes(chunk));
                logger.info("archived {} objects ({}-{}) into chunk '{}'", chunk.size(), start, end, name);
                start = end;
                keys = objects.keys(namespace, start, chunkSize);
            }
        }
    }

    protected void doRestore(final Objects objects, final String namespace, final Path archiveFile) throws IOException {
        // create the namespace, in case the user hasn't already
        objects.create(namespace);
        try (final ArchiveInputStream archive = getArchiveInputStream(archiveFile)) {
            ArchiveEntry entry;
            int total = 0;
            while ((entry = archive.getNextEntry()) != null) {
                final ObjectsChunk chunk = ObjectsChunk.parseFrom(archive);
                for (final Map.Entry<String, ByteString> chunkEntry : chunk.getObjectsMap().entrySet()) {
                    objects.store(namespace, chunkEntry.getKey(), chunkEntry.getValue().toByteArray());
                }
                logger.info("read {} objects from chunk {} ({} bytes) into {}", chunk.getObjectsCount(), entry.getName(), entry.getSize(), objects);
                total += chunk.getObjectsCount();
            }
            logger.info("restored {} objects into namespace '{}' from archive file {}", total, namespace, archiveFile);
        }
    }

    protected byte[] getBytes(final Map<String, byte[]> chunk) {
        final ObjectsChunk.Builder builder = ObjectsChunk.newBuilder();
        // have to convert all byte arrays into proto byte strings
        for (final Map.Entry<String, byte[]> entry : chunk.entrySet()) {
            builder.putObjects(entry.getKey(), ByteString.copyFrom(entry.getValue()));
        }
        return builder.build().toByteArray();
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
