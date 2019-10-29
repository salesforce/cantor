package com.salesforce.cantor.archive;

import com.google.protobuf.ByteString;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.Sets;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.common.CommonPreconditions.checkString;

public class CantorArchiver {
    private static final Logger logger = LoggerFactory.getLogger(CantorArchiver.class);

    private static final int maxObjectChunkSize = 1_000;
    private static final int maxSetsChunkSize = 1_000;

    public static void archive(final Objects objects, final String namespace, final Path destination) throws IOException {
        checkArchiveArguments(objects, namespace, destination);
        try (final ArchiveOutputStream archive = getArchiveOutputStream(destination)) {
            // get objects to archive in chunks in case of large namespaces
            int start = 0;
            Collection<String> keys = objects.keys(namespace, start, maxObjectChunkSize);
            while (!keys.isEmpty()) {
                final Map<String, byte[]> chunk = objects.get(namespace, keys);
                final int end = start + chunk.size();
                final String name = String.format("objects-%s-%s-%s", namespace, start, end);
                // store chunks as tar archives so we can restore them in chunks too
                writeArchiveEntry(archive, name, getBytes(chunk));
                logger.info("archived {} objects ({}-{}) into chunk '{}'", chunk.size(), start, end, name);
                start = end;
                keys = objects.keys(namespace, start, maxObjectChunkSize);
            }
        }
    }

    public static void archive(final Sets sets, final String namespace, final Path destination) throws IOException {
        checkArchiveArguments(sets, namespace, destination);
        // get all sets for the namespace, any sets added after won't be archived
        final Collection<String> setNames = sets.sets(namespace);
        try (final ArchiveOutputStream archive = getArchiveOutputStream(destination)) {
            // archive each set one at a time
            for (final String set : setNames) {
                logger.info("archiving set {}.{}", namespace, set);
                int start = 0;
                Map<String, Long> entries = sets.get(namespace, set, start, maxSetsChunkSize);
                while (!entries.isEmpty()) {
                    final int end = start + entries.size();
                    final String name = String.format("sets-%s-%s-%s-%s", namespace, set, start, end);
                    // store chunks as tar archives so we can restore them in chunks too
                    final SetsChunk chunk = SetsChunk.newBuilder().setSet(set).putAllEntries(entries).build();
                    writeArchiveEntry(archive, name, chunk.toByteArray());
                    logger.info("archived {} entries ({}-{}) into chunk '{}' for set {}.{}", entries.size(), start, end, name, namespace, set);
                    start = end;
                    entries = sets.get(namespace, set, start, maxSetsChunkSize);
                }
            }
        }
    }

    public static void restore(final Objects objects, final String namespace, final Path archiveFile) throws IOException {
        checkRestoreArguments(objects, namespace, archiveFile);
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

    private static void writeArchiveEntry(final ArchiveOutputStream archive, final String name, final byte[] bytes) throws IOException {
        final TarArchiveEntry entry = new TarArchiveEntry(name);
        entry.setSize(bytes.length);
        archive.putArchiveEntry(entry);
        archive.write(bytes);
        archive.closeArchiveEntry();
    }

    private static byte[] getBytes(final Map<String, byte[]> chunk) {
        final ObjectsChunk.Builder builder = ObjectsChunk.newBuilder();
        // have to convert all byte arrays into proto byte strings
        for (final Map.Entry<String, byte[]> entry : chunk.entrySet()) {
            builder.putObjects(entry.getKey(), ByteString.copyFrom(entry.getValue()));
        }
        return builder.build().toByteArray();
    }

    private static ArchiveOutputStream getArchiveOutputStream(final Path destination) throws IOException {
        return new TarArchiveOutputStream(new GzipCompressorOutputStream(new BufferedOutputStream(Files.newOutputStream(destination))));
    }

    private static ArchiveInputStream getArchiveInputStream(final Path archiveFile) throws IOException {
        return new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(Files.newInputStream(archiveFile))));
    }

    private static void checkArchiveArguments(final Object instance, final String namespace, final Path destination) {
        checkArgument(instance != null, "null cantor instance, can't archive");
        checkString(namespace, "null/empty namespace, can't archive");
        checkArgument(Files.notExists(destination), "destination already exists, can't archive");
    }

    private static void checkRestoreArguments(final Object instance, final String namespace, final Path archiveFile) {
        checkArgument(instance != null, "null objects, can't restore");
        checkString(namespace, "null/empty namespace, can't restore");
        checkArgument(Files.exists(archiveFile), "can't locate archive file, can't restore");
    }
}
