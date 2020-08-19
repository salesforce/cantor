/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive.s3;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.archive.file.EventsArchiverOnFile;
import com.salesforce.cantor.misc.archivable.EventsArchiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

public class EventsArchiverOnS3 extends EventsArchiverOnFile implements EventsArchiver {
    private static final Logger logger = LoggerFactory.getLogger(EventsArchiverOnS3.class);
    private static final String archiveNamespace = "events-archive";

    private final Cantor cantorOnS3;

    public EventsArchiverOnS3(final Cantor cantorOnS3,
                              final String baseDirectory) throws IOException {
        super(baseDirectory);
        logger.info("initializing s3 events archiver");
        this.cantorOnS3 = cantorOnS3;
        this.cantorOnS3.objects().create(archiveNamespace);
        // upload any archive files that are sitting in local storage
        final Path archiveLocation = super.getArchiveLocation();
        final List<Path> archiveFiles = Files.list(archiveLocation)
                .filter(Files::isRegularFile)
                .collect(Collectors.toList());
        for (final Path file : archiveFiles) {
            uploadToS3(file.getFileName().toString(), file);
            // delete temporary storage file
            if (!file.toFile().delete()) {
                logger.warn("failed to delete temp archive file during startup {}", file);
            }
        }
    }

    @Override
    public void archive(final Events events,
                        final String namespace,
                        final long startTimestampMillis,
                        final long endTimestampMillis,
                        final Map<String, String> metadataQuery,
                        final Map<String, String> dimensionsQuery) throws IOException {
        // temporary map of local archive files to check if they've been modified
        final Map<String, Long> fileToModifiedTime = new HashMap<>();

        // ensuring that any archive files for this time period that already exist will be merged and not lost
        final Collection<String> archiveFilenames = this.cantorOnS3.objects().keys(archiveNamespace, 0, -1);
        final List<String> archives = getMatchingArchives(namespace, archiveFilenames, startTimestampMillis, endTimestampMillis);
        final Path archiveLocation = super.getArchiveLocation();
        for (final String objectKey : archives) {
            logger.debug("objectKey already exists, pulling to merge: {}", objectKey);
            final Path fileLocation = archiveLocation.resolve(objectKey);
            pullFile(objectKey, fileLocation);
            fileToModifiedTime.put(objectKey, fileLocation.toFile().lastModified());
        }

        // first archive to the local disk
        super.archive(events, namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);

        // iterate over all archive files and upload them to s3
        for (final Path archiveFile : super.getFileArchiveList(namespace, startTimestampMillis, endTimestampMillis)) {
            // only upload if the file was actually modified
            final Long timeModified = fileToModifiedTime.get(archiveFile.getFileName().toString());
            if (timeModified == null || timeModified != archiveFile.toFile().lastModified()) {
                uploadToS3(archiveFile.getFileName().toString(), archiveFile);
            }

            // delete temporary storage file
            if (!archiveFile.toFile().delete()) {
                logger.warn("failed to delete temp archive file {}", archiveFile);
            }
        }
    }

    @Override
    public void restore(final Events events,
                        final String namespace,
                        final long startTimestampMillis,
                        final long endTimestampMillis) throws IOException {
        final Collection<String> archiveFilenames = this.cantorOnS3.objects().keys(archiveNamespace, 0, -1);
        final List<String> archives = getMatchingArchives(namespace, archiveFilenames, startTimestampMillis, endTimestampMillis);
        // TODO: should we run this in parallel?
        // logging at this level will be handled by the fileArchiver
        final Path archiveLocation = super.getArchiveLocation();
        for (final String archiveObjectName : archives) {
            final Path archiveFile = archiveLocation.resolve(archiveObjectName);
            // no need to restore a file already pulled down
            if (pullFile(archiveObjectName, archiveFile)) {
                super.doRestore(events, namespace, archiveFile);
            }
        }
    }

    // takes any file and uploads it to S3
    public void uploadToS3(final String objectKey, final Path archiveFile) throws IOException {
        checkArgument(Files.exists(archiveFile) && archiveFile.toFile().length() != 0, "file doesn't exist or is empty: " + archiveFile);
        int byteSize = 0;
        long startNanos = System.nanoTime();
        try (final InputStream uploadStream = Files.newInputStream(archiveFile)) {
            byteSize = uploadStream.available();
            final byte[] fileBytes = new byte[byteSize];
            final int read = uploadStream.read(fileBytes);
            if (read != byteSize) {
                throw new IOException(String.format("failed to read all bytes into memory %d/%d read", read, byteSize));
            }
            // TODO: streams aren't working seemingly hitting their max buffer size causing a amazonaws.ResetException
            this.cantorOnS3.objects().store(archiveNamespace, objectKey, fileBytes);
        } finally {
            logger.info("uploading file '{}' ({} bytes) took {}s",
                    archiveFile, byteSize, TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos));
        }
    }

    // the archive file could potential already be on disk, but if not we need to get it from S3
    public boolean pullFile(final String objectKey, final Path archiveLocation) throws IOException {
        // check for archive stored on disk
        // TODO: add checksum evaluation to confirm the file currently pulled down is not outdated
        if (archiveLocation.toFile().exists()) {
            return false;
        }

        // pulling file down from s3
        logger.debug("local file not found, pulling file from s3: {}", objectKey);
        final byte[] fileBytes = this.cantorOnS3.objects().get(archiveNamespace, objectKey);
        try (final OutputStream temporaryStorage = Files.newOutputStream(archiveLocation)) {
            temporaryStorage.write(fileBytes);
            temporaryStorage.flush();
        }
        return true;
    }
}
