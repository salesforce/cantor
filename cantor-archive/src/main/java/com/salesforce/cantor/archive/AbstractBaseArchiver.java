/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive;

import com.salesforce.cantor.misc.CantorProperties;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.common.CommonPreconditions.checkString;

abstract class AbstractBaseArchiver {
    private static final String archivePrefixFormat = "archive-%s-%s-";

    static Path createTempFile(final String namespace) throws IOException {
        return File.createTempFile(String.format(archivePrefixFormat, CantorProperties.getKingdom(), namespace), null).toPath();
    }

    static void writeArchiveEntry(final ArchiveOutputStream archive, final String name, final byte[] bytes) throws IOException {
        final TarArchiveEntry entry = new TarArchiveEntry(name);
        entry.setSize(bytes.length);
        archive.putArchiveEntry(entry);
        archive.write(bytes);
        archive.closeArchiveEntry();
    }

    static ArchiveOutputStream getArchiveOutputStream(final Path destination) throws IOException {
        return new TarArchiveOutputStream(new GzipCompressorOutputStream(new BufferedOutputStream(Files.newOutputStream(destination))));
    }

    static ArchiveInputStream getArchiveInputStream(final Path archiveFile) throws IOException {
        return new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(Files.newInputStream(archiveFile))));
    }

    static void checkArchiveArguments(final Object instance, final String namespace, final Path destination) throws IOException {
        checkArgument(instance != null, "null cantor instance, can't archive");
        checkString(namespace, "null/empty namespace, can't archive");
        checkArgument(Files.notExists(destination) || Files.size(destination) == 0, "destination already exists and is not empty, can't archive");
    }

    static void checkRestoreArguments(final Object instance, final String namespace, final Path archiveFile) {
        checkArgument(instance != null, "null objects, can't restore");
        checkString(namespace, "null/empty namespace, can't restore");
        checkArgument(Files.exists(archiveFile), "can't locate archive file, can't restore");
    }
}
