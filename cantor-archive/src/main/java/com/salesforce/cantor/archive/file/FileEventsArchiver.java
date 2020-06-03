/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive.file;

import com.google.protobuf.ByteString;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.archive.EventsChunk;
import com.salesforce.cantor.common.EventsPreconditions;
import com.salesforce.cantor.misc.CantorProperties;
import com.salesforce.cantor.misc.archivable.EventsArchiver;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

public class FileEventsArchiver extends AbstractBaseFileArchiver implements EventsArchiver {
    private static final Logger logger = LoggerFactory.getLogger(FileEventsArchiver.class);
    private static final String archivePathFormat = "/archive-events-%s-%s-%d-%d";

    public static final long MIN_CHUNK_MILLIS = TimeUnit.MINUTES.toMillis(1);
    public static final long MAX_CHUNK_MILLIS = TimeUnit.MINUTES.toMillis(60);


    public FileEventsArchiver(final String baseDirectory, final long chunkMillis) {
        super(baseDirectory, chunkMillis);
    }

    @Override
    public void archive(final Events events,
                        final String namespace,
                        final long endTimestampMillis) throws IOException {
        archive(events, namespace, 0, endTimestampMillis, null, null);
    }

    @Override
    public void archive(final Events events,
                        final String namespace,
                        final long startTimestampMillis,
                        final long endTimestampMillis,
                        final Map<String, String> metadataQuery,
                        final Map<String, String> dimensionsQuery) throws IOException {
        long startNanos = System.nanoTime();
        long totalEventsArchived = 0;
        try {
            for (long end = getFloorForWindow(endTimestampMillis, this.chunkMillis) - 1, start = (end + 1) - chunkMillis;
                 end > 0;
                 end -= this.chunkMillis, start -= this.chunkMillis) {
                final long archivedEvents = doArchive(
                        events, namespace,
                        start, end,
                        metadataQuery, dimensionsQuery,
                        getFileArchive(namespace, start));
                totalEventsArchived += archivedEvents;
                if (archivedEvents == 0 && events.first(namespace, startTimestampMillis, start) == null) {
                    // no more events left to archive
                    return;
                }
            }
        } finally {
            logger.info("archiving {} events for namespace '{}' took {}s",
                    totalEventsArchived, namespace, TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos));
        }
    }

    @Override
    public void restore(final Events events,
                        final String namespace,
                        final long startTimestampMillis,
                        final long endTimestampMillis) throws IOException {
        long startNanos = System.nanoTime();
        long chunkTotal = 0;
        long totalEventsRestored = 0;
        try {
            for (long start = getFloorForWindow(startTimestampMillis, this.chunkMillis), end = start + chunkMillis - 1;
                 start <= endTimestampMillis;
                 start += this.chunkMillis, end += this.chunkMillis) {
                final Path archiveFile = getFileArchive(namespace, start);
                chunkTotal++;
                totalEventsRestored += doRestore(events, namespace, archiveFile);
            }
        } finally {
            logger.info("restoring {} chucks, {} events for namespace '{}' took {}s",
                    chunkTotal, totalEventsRestored, namespace, TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos));
        }
    }

    public long doArchive(final Events events,
                          final String namespace,
                          final long startTimestampMillis,
                          final long endTimestampMillis,
                          final Map<String, String> metadataQuery,
                          final Map<String, String> dimensionsQuery,
                          final Path destination) throws IOException {
        checkArchiveArguments(events, namespace, destination);
        checkArgument(this.chunkMillis >= MIN_CHUNK_MILLIS, "archive chunk millis must be greater than " + MIN_CHUNK_MILLIS);
        checkArgument(this.chunkMillis <= MAX_CHUNK_MILLIS, "archive chunk millis must be less than " + MAX_CHUNK_MILLIS);
        EventsPreconditions.checkGet(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);

        long startNanos = System.nanoTime();
        long eventsArchived = 0;
        try (final ArchiveOutputStream archive = getArchiveOutputStream(destination)) {
            final List<Events.Event> chunkEvents = events.get(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery, true, true, 0);
            if (chunkEvents.size() == 0) {
                // exit if no events
                return eventsArchived;
            }
            // todo: can we do this differently? This doubles the memory we hold on to :(
            final EventsChunk.Builder chunkBuilder = EventsChunk.newBuilder();
            for (final Events.Event event : chunkEvents) {
                final EventsChunk.Event.Builder eventBuilder = EventsChunk.Event.newBuilder()
                        .setTimestampMillis(event.getTimestampMillis())
                        .putAllDimensions(event.getDimensions())
                        .putAllMetadata(event.getMetadata());
                if (event.getPayload() != null && event.getPayload().length > 0) {
                    eventBuilder.setPayload(ByteString.copyFrom(event.getPayload()));
                }
                chunkBuilder.addEvents(eventBuilder.build());
            }
            writeArchiveEntry(archive, destination.getFileName().toString(), chunkBuilder.build().toByteArray());
            eventsArchived += chunkEvents.size();
            return eventsArchived;
        } finally {
            logger.info("archiving {}ms chunk for namespace '{}' took {}s",
                    this.chunkMillis, namespace, TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos));
        }
    }

    public long doRestore(final Events events,
                          final String namespace,
                          final Path archiveFile) throws IOException {
        checkRestoreArguments(events, namespace, archiveFile);
        // create the namespace, in case the user hasn't already
        // TODO: potential bug here; seeing data deletion when creating a namespace that already exists
        events.create(namespace);

        long startNanos = System.nanoTime();
        long eventsRestored = 0;
        try (final ArchiveInputStream archive = getArchiveInputStream(archiveFile)) {
            ArchiveEntry entry;
            while ((entry = archive.getNextEntry()) != null) {
                final EventsChunk chunk = EventsChunk.parseFrom(archive);
                for (final EventsChunk.Event event : chunk.getEventsList()) {
                    if (ByteString.EMPTY.equals(event.getPayload())) {
                        events.store(namespace, event.getTimestampMillis(), event.getMetadataMap(), event.getDimensionsMap());
                    } else {
                        events.store(namespace, event.getTimestampMillis(), event.getMetadataMap(), event.getDimensionsMap(), event.getPayload().toByteArray());
                    }
                }
                logger.info("read {} entries from chunk {} ({} bytes) into {}", chunk.getEventsCount(), entry.getName(), entry.getSize(), namespace);
                eventsRestored += chunk.getEventsCount();
            }
            return eventsRestored;
        } finally {
            logger.info("restoring {} events into namespace '{}' from archive file {} took {}s",
                    eventsRestored, namespace, archiveFile, TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos));
        }
    }

    public Path getFileArchive(final String namespace, final long startTimestampMillis) {
        return getDirectory(archivePathFormat,
                CantorProperties.getKingdom(),
                namespace,
                startTimestampMillis,
                startTimestampMillis + chunkMillis - 1);
    }
}
