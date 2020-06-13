/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive.s3;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.archive.file.ArchiverOnFile;
import com.salesforce.cantor.archive.file.EventsArchiverOnFile;
import com.salesforce.cantor.misc.archivable.CantorArchiver;
import com.salesforce.cantor.misc.archivable.EventsArchiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class EventsArchiverOnS3 extends AbstractBaseArchiverOnS3 implements EventsArchiver {
    private static final Logger logger = LoggerFactory.getLogger(EventsArchiverOnS3.class);
    private static final Pattern archiveRegexPattern = Pattern.compile("archive-events-(?<namespace>.*)-(?<start>\\d+)-(?<end>\\d+)");
    private static final String archiveNamespace = "events-archive";

    final EventsArchiverOnFile eventsArchiverOnFile;

    public EventsArchiverOnS3(final Cantor cantorOnS3,
                              final CantorArchiver fileArchiver,
                              final long chunkMillis) throws IOException {
        super(cantorOnS3, fileArchiver, chunkMillis);
        logger.info("events archiver chucking in {}ms chunks", chunkMillis);
        this.eventsArchiverOnFile = (EventsArchiverOnFile) fileArchiver.events();
        this.cantorOnS3.objects().create(archiveNamespace);
    }

    @Override
    public void archive(final Events events,
                        final String namespace,
                        final long startTimestampMillis,
                        final long endTimestampMillis,
                        final Map<String, String> metadataQuery,
                        final Map<String, String> dimensionsQuery) throws IOException {
        final Collection<String> archiveFilenames = this.cantorOnS3.objects().keys(archiveNamespace, 0, -1);
        final List<String> archives = getMatchingArchives(namespace, archiveFilenames, startTimestampMillis, endTimestampMillis);
        final Path archiveLocation = ((ArchiverOnFile) this.fileArchiver).getArchiveLocation();
        for (final String objectKey : archives) {
            logger.debug("objectKey already exists, pulling to merge: {}", objectKey);
            pullFile(objectKey, archiveLocation.resolve(objectKey));
        }

        // first archive to the local disk
        this.eventsArchiverOnFile.archive(
                events, namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery
        );

        // iterate over all archive files and upload them to s3
        for (final Path archiveFile : this.eventsArchiverOnFile.getFileArchiveList(namespace, startTimestampMillis, endTimestampMillis)) {
            doArchive(archiveFile.getFileName().toString(), archiveFile);
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
        final Path archiveLocation = ((ArchiverOnFile) this.fileArchiver).getArchiveLocation();
        for (final String archiveObjectName : archives) {
            final Path archiveFile = archiveLocation.resolve(archiveObjectName);
            // no need to restore a file already
            if (pullFile(archiveObjectName, archiveFile)) {
                ((EventsArchiverOnFile) this.fileArchiver.events()).doRestore(events, namespace, archiveFile);
            }
        }
    }

    public void doArchive(final String objectKey,
                          final Path archiveFile) throws IOException {
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

    public List<String> getMatchingArchives(final String namespace,
                                            final Collection<String> archiveFilenames,
                                            final long startTimestampMillis,
                                            final long endTimestampMillis) {
        final long windowStart = getFloorForChunk(startTimestampMillis);
        final long windowEnd = (endTimestampMillis <= Long.MAX_VALUE - this.chunkMillis)
                ? getCeilingForChunk(endTimestampMillis)
                : endTimestampMillis;
        return archiveFilenames.stream()
                .filter(filename -> {
                    // filter to archive files that overlap with the timeframe
                    final Matcher matcher = archiveRegexPattern.matcher(filename);
                    if (matcher.matches() && matcher.group("namespace").equals(namespace)) {
                        final long fileStart = Long.parseLong(matcher.group("start"));
                        final long fileEnd = Long.parseLong(matcher.group("end"));
                        return fileStart <= windowEnd && fileEnd >= windowStart;
                    }
                    return false;
                }).collect(Collectors.toList());
    }

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
