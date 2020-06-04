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
import com.salesforce.cantor.misc.CantorProperties;
import com.salesforce.cantor.misc.archivable.ObjectsArchiver;
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

public class FileObjectsArchiver extends AbstractBaseFileArchiver implements ObjectsArchiver {
    private static final Logger logger = LoggerFactory.getLogger(FileObjectsArchiver.class);
    protected static final String archivePathFormat = "/archive-objects-%s-%s-%d";

    public static final int MAX_CHUNK_SIZE = 1_000;

    public FileObjectsArchiver(final String baseDirectory, final int archiveChunkCount) {
        super(baseDirectory, archiveChunkCount);
    }

    @Override
    public void archive(final Objects objects, final String namespace) throws IOException {
        final Path destination = getFileArchive(namespace);
        checkArchiveArguments(objects, namespace, destination);
        checkArgument(this.chunkCount <= MAX_CHUNK_SIZE, "chunk size must be <=" + MAX_CHUNK_SIZE);
        doArchive(objects, namespace, destination);
    }

    @Override
    public void restore(final Objects objects, final String namespace) throws IOException {
        final Path archiveFile = getFileArchive(namespace);
        checkRestoreArguments(objects, namespace, archiveFile);
        doRestore(objects, namespace, archiveFile);
    }

    public void doArchive(final Objects objects, final String namespace, final Path destination) throws IOException {
        try (final ArchiveOutputStream archive = getArchiveOutputStream(destination)) {
            // get objects to archive in chunks in case of large namespaces
            int start = 0;
            Collection<String> keys = objects.keys(namespace, start, this.chunkCount);
            while (!keys.isEmpty()) {
                final Map<String, byte[]> chunk = objects.get(namespace, keys);
                final int end = start + chunk.size();
                final String name = String.format("objects-%s-%s-%s", namespace, start, end);
                // store chunks as tar archives so we can restore them in chunks too
                writeArchiveEntry(archive, name, getBytes(chunk));
                logger.info("archived {} objects ({}-{}) into chunk '{}'", chunk.size(), start, end, name);
                start = end;
                keys = objects.keys(namespace, start, this.chunkCount);
            }
        }
    }

    public void doRestore(final Objects objects, final String namespace, final Path archiveFile) throws IOException {
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

    private byte[] getBytes(final Map<String, byte[]> chunk) {
        final ObjectsChunk.Builder builder = ObjectsChunk.newBuilder();
        // have to convert all byte arrays into proto byte strings
        for (final Map.Entry<String, byte[]> entry : chunk.entrySet()) {
            builder.putObjects(entry.getKey(), ByteString.copyFrom(entry.getValue()));
        }
        return builder.build().toByteArray();
    }

    public Path getFileArchive(final String namespace) {
        return getFile(archivePathFormat,
                CantorProperties.getKingdom(),
                namespace,
                this.chunkCount);
    }
}
