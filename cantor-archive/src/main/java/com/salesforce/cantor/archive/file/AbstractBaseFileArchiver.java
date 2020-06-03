package com.salesforce.cantor.archive.file;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.common.CommonPreconditions.checkString;

public abstract class AbstractBaseFileArchiver {
    protected final String baseDirectory;
    protected final int chunkCount;
    protected final long chunkMillis;

    // default to nothing for no sub directory
    protected String subDirectory = "";

    public AbstractBaseFileArchiver(final String baseDirectory, final long chunkMillis) {
        this(baseDirectory, 0, chunkMillis);
    }

    public AbstractBaseFileArchiver(final String baseDirectory, final int chunkCount) {
        this(baseDirectory, chunkCount, 0);
    }

    public AbstractBaseFileArchiver(final String baseDirectory, final int chunkCount, final long chunkMillis) {
        this.baseDirectory = baseDirectory;
        this.chunkCount = chunkCount;
        this.chunkMillis = chunkMillis;
    }

    public Path getDirectory(final String fileNameFormat, final Object... args) {
        return Paths.get(this.baseDirectory, this.subDirectory, String.format(fileNameFormat, args));
    }

    public void setSubDirectory(final String subDirectory) {
        this.subDirectory = (subDirectory != null) ? subDirectory : "";
    }

    protected void writeArchiveEntry(final ArchiveOutputStream archive, final String name, final byte[] bytes) throws IOException {
        if (bytes.length == 0) {
            return;
        }

        final TarArchiveEntry entry = new TarArchiveEntry(name);
        entry.setSize(bytes.length);
        archive.putArchiveEntry(entry);
        archive.write(bytes);
        archive.closeArchiveEntry();
    }

    protected ArchiveOutputStream getArchiveOutputStream(final Path destination) throws IOException {
        return new TarArchiveOutputStream(new GzipCompressorOutputStream(new BufferedOutputStream(Files.newOutputStream(destination))));
    }

    protected ArchiveInputStream getArchiveInputStream(final Path archiveFile) throws IOException {
        return new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(Files.newInputStream(archiveFile))));
    }

    protected void checkArchiveArguments(final Object instance, final String namespace, final Path destination) throws IOException {
        checkArgument(instance != null, "null cantor instance, can't archive");
        checkString(namespace, "null/empty namespace, can't archive");
        checkArgument(Files.notExists(destination) || Files.size(destination) == 0, "file already exists and is not empty, can't archive");
    }

    protected void checkRestoreArguments(final Object instance, final String namespace, final Path archiveFile) {
        checkArgument(instance != null, "null objects, can't restore");
        checkString(namespace, "null/empty namespace, can't restore");
        checkArgument(Files.exists(archiveFile), "can't locate archive file, can't restore");
    }

    protected long getFloorForWindow(final long timestampMillis, final long chunkMillis) {
        return (timestampMillis / chunkMillis) * chunkMillis;
    }
}
