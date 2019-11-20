/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive;

import com.google.protobuf.ByteString;
import com.salesforce.cantor.Events;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

public class EventsArchiver extends AbstractBaseArchiver {
    private static final Logger logger = LoggerFactory.getLogger(EventsArchiver.class);

    public static final long MIN_CHUNK_MILLIS = TimeUnit.MINUTES.toMillis(1);
    public static final long MAX_CHUNK_MILLIS = TimeUnit.MINUTES.toMillis(60);

    public static void archive(final Events events, final String namespace, final long startTimestampMillis,
                               final long endTimestampMillis, final long chunkMillis, final Path destination) throws IOException {
        checkArchiveArguments(events, namespace, destination);
        checkArgument(startTimestampMillis > 0, "start timestamp must be positive");
        checkArgument(startTimestampMillis < endTimestampMillis, "start timestamp must be before end timestamp");
        checkArgument(chunkMillis >= MIN_CHUNK_MILLIS, "archive chunk millis must be greater than " + MIN_CHUNK_MILLIS);
        checkArgument(chunkMillis <= MAX_CHUNK_MILLIS, "archive chunk millis must be less than " + MAX_CHUNK_MILLIS);

        try (final ArchiveOutputStream archive = getArchiveOutputStream(destination)) {
            long chunkStart = startTimestampMillis;
            long chunkEnd = Math.min(chunkStart + chunkMillis, endTimestampMillis);
            while (chunkStart < endTimestampMillis) {
                final String name = String.format("events-%s-%s-%s", namespace, chunkStart, chunkEnd);
                final List<Events.Event> chunkEvents = events.get(namespace, chunkStart, chunkEnd, true);
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
                writeArchiveEntry(archive, name, chunkBuilder.build().toByteArray());
                chunkStart = chunkEnd;
                chunkEnd = Math.min(chunkStart + chunkMillis, endTimestampMillis);
            }
        }
    }

    public static void restore(final Events events, final String namespace, final Path archiveFile) throws IOException {
        checkRestoreArguments(events, namespace, archiveFile);
        // create the namespace, in case the user hasn't already
        events.create(namespace);
        try (final ArchiveInputStream archive = getArchiveInputStream(archiveFile)) {
            ArchiveEntry entry;
            int total = 0;
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
                total += chunk.getEventsCount();
            }
            logger.info("restored {} events into namespace '{}' from archive file {}", total, namespace, archiveFile);
        }
    }
}
