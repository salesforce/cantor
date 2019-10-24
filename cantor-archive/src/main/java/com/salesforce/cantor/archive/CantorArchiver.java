package com.salesforce.cantor.archive;

import com.google.protobuf.ByteString;
import com.salesforce.cantor.Objects;
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
import java.util.Map;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.common.CommonPreconditions.checkString;

public class CantorArchiver {
    private static final Logger logger = LoggerFactory.getLogger(CantorArchiver.class);

    private static final int maxObjectChunkSize = 1_000;

    public static void archive(final Objects objects, final String namespace, final Path destination) throws IOException {
        checkArgument(objects != null, "null objects, can't archive");
        checkString(namespace, "null/empty namespace, can't archive");
        checkArgument(Files.notExists(destination), "destination already exists, can't archive");

        final int size = objects.size(namespace);
        try (final ArchiveOutputStream archive = getArchiveOutputStream(destination)) {
            // todo: add metadata tar entry?

            // get objects to archive in chunks in case of large namespaces
            int start = 0;
            while (start != size) {
                final Map<String, byte[]> chunk = objects.get(namespace, objects.keys(namespace, start, maxObjectChunkSize));
                final int end = start + chunk.size();
                final String name = String.format("objects-%s-%s-%s", namespace, start, end);
                // store chunks as tar archives so we can restore them in chunks too
                writeArchiveEntry(archive, name, getBytes(chunk));
                logger.info("archived {} objects ({}-{} out of {}) into chunk '{}'", chunk.size(), start, end, size, name);
                start = end;
            }
        }
    }

    public static void restore(final Objects objects, final String namespace, final Path archiveFile) throws IOException {
        checkArgument(objects != null, "null objects, can't restore");
        checkString(namespace, "null/empty namespace, can't restore");
        checkArgument(Files.exists(archiveFile), "can't locate archive file, can't restore");

        // todo: should we create this namespace? or require the user create this themselves?
        objects.create(namespace);
        try (final ArchiveInputStream archive = getArchiveInputStream(archiveFile)) {
            ArchiveEntry entry;
            int total = 0;
            while ((entry = archive.getNextEntry()) != null) {
                final ObjectsChunk chunk = ObjectsChunk.parseFrom(archive);
                // todo: store these all at once? converting will double memory we hold onto, but will store quicker
                for (final Map.Entry<String, ByteString> chunkEntry : chunk.getObjectsMap().entrySet()) {
                    objects.store(namespace, chunkEntry.getKey(), chunkEntry.getValue().toByteArray());
                }
                logger.info("read {} objects from chunk {} ({} bytes) into {}", chunk.getObjectsCount(), entry.getName(), entry.getSize(), objects);
                total += chunk.getObjectsCount();
            }
            logger.info("restored {} objects into namespace '{}' from archive file {}", total, namespace, archiveFile);
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

}
