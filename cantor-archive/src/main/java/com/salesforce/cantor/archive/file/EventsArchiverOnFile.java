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
import com.salesforce.cantor.misc.archivable.EventsArchiver;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.common.CommonPreconditions.checkNamespace;

public class EventsArchiverOnFile extends AbstractBaseArchiverOnFile implements EventsArchiver {
    private static final Logger logger = LoggerFactory.getLogger(EventsArchiverOnFile.class);
    private static final String archivePathFormat = "/archive-events-%s-%d-%d";
    private static final Pattern archiveRegexPattern = Pattern.compile("archive-events-(?<namespace>.*)-(?<start>\\d+)-(?<end>\\d+)");

    private static final String isRestoredFlag = ".cantor-archive-restored";
    private static final long minChunkMillis = TimeUnit.MINUTES.toMillis(1);
    private static final long maxChunkMillis = TimeUnit.DAYS.toMillis(1);

    public EventsArchiverOnFile(final String baseDirectory, final long chunkMillis) {
        super(baseDirectory, chunkMillis);
        checkArgument(this.chunkMillis >= minChunkMillis, "archive chunk millis must be greater than " + minChunkMillis);
        checkArgument(this.chunkMillis <= maxChunkMillis, "archive chunk millis must be less than " + maxChunkMillis);
    }

    // partitioning and file naming logic is done here followed by the actual archiving logic in doArchive
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
            for (long start = getFloorForChunk(endTimestampMillis), end = endTimestampMillis;
                 end > 0;
                 end -= this.chunkMillis, start -= this.chunkMillis) {
                final long archivedEvents = doArchive(
                        events, namespace,
                        Math.max(start, startTimestampMillis), end,
                        metadataQuery, dimensionsQuery,
                        getFileArchive(namespace, start));
                totalEventsArchived += archivedEvents;
                final long floorForStart = getFloorForChunk(startTimestampMillis);
                if (archivedEvents == 0 && events.first(namespace, floorForStart + this.chunkMillis, start) == null) {
                    // TODO: build a heuristic to jump to the next chunk with events to archive instead of this hack to handle events with zero for a timestamp
                    if (events.first(namespace, floorForStart, floorForStart + this.chunkMillis - 1) != null) {
                        start = floorForStart + this.chunkMillis;
                        end = floorForStart + (this.chunkMillis * 2) - 1;
                        continue;
                    }
                    // no more events left to archive
                    return;
                }

                if (end == endTimestampMillis) {
                    // after first partial archive archive full chunks
                    end = getCeilingForChunk(end) - 1;
                }
            }
        } finally {
            logger.info("archiving {} events for namespace '{}' took {}s",
                    totalEventsArchived, namespace, TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos));
        }
    }

    // partitioning and file resolution logic is done here followed by the actual restoration logic in doRestore
    @Override
    public void restore(final Events events,
                        final String namespace,
                        final long startTimestampMillis,
                        final long endTimestampMillis) throws IOException {
        checkNamespace(namespace);
        checkArgument(startTimestampMillis >= 0, "invalid start timestamp");
        checkArgument(endTimestampMillis >= startTimestampMillis, "end timestamp cannot be before start timestamp");

        final List<Path> archives = getFileArchiveList(namespace, startTimestampMillis, endTimestampMillis);
        long startNanos = System.nanoTime();
        long totalEventsRestored = 0;
        try {
            for (final Path archive : archives) {
                totalEventsRestored += doRestore(events, namespace, archive);
            }
        } finally {
            logger.info("restoring {} chunks, {} events for namespace '{}' took {}s",
                    archives.size(), totalEventsRestored, namespace, TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos));
        }
    }

    // real archiving logic which will archive the entire timeframe matching the query into the provided file
    public long doArchive(final Events events,
                          final String namespace,
                          final long startTimestampMillis,
                          final long endTimestampMillis,
                          final Map<String, String> metadataQuery,
                          final Map<String, String> dimensionsQuery,
                          final Path destination) throws IOException {
        EventsPreconditions.checkGet(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        long startNanos = System.nanoTime();
        long eventsArchived = 0;

        final List<Events.Event> chunkEvents = events.get(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery, true, true, 0);
        if (chunkEvents.size() == 0) {
            // exit if no events
            return eventsArchived;
        }

        final EventsChunk.Builder chunkBuilder = EventsChunk.newBuilder();
        if (!checkArchiveArguments(events, namespace, destination)) {
            logger.debug("file already exists and is not empty, pulling to merge: {}", destination);
            try (final ArchiveInputStream archive = getArchiveInputStream(destination)) {
                while (archive.getNextEntry() != null) {
                    chunkBuilder.mergeFrom(archive);
                }
            }
        }

        try (final ArchiveOutputStream archive = getArchiveOutputStream(destination)) {
            // todo: can we do this differently? This doubles the memory we hold on to :(
            for (final Events.Event event : chunkEvents) {
                final double restoredEvent = event.getDimensions().getOrDefault(isRestoredFlag, 0d);
                if (restoredEvent > 0) {
                    // skip elements that have already been restored to prevent duplication
                    continue;
                }

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
                    endTimestampMillis - startTimestampMillis,
                    namespace,
                    TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos));
        }
    }

    // real restoration logic which takes any archive file provided and loads the entire contents into events under the given namespace
    public long doRestore(final Events events,
                          final String namespace,
                          final Path archiveFile) throws IOException {
        checkRestoreArguments(events, namespace, archiveFile);
        // create the namespace, in case the user hasn't already
        // TODO: potential bug here; seeing data deletion when creating a namespace that already exists
        events.create(namespace);
        cleanRestoredEvents(events, namespace, archiveFile);

        long startNanos = System.nanoTime();
        long eventsRestored = 0;
        try (final ArchiveInputStream archive = getArchiveInputStream(archiveFile)) {
            ArchiveEntry entry;
            while ((entry = archive.getNextEntry()) != null) {
                final EventsChunk chunk = EventsChunk.parseFrom(archive);
                events.store(namespace, toEvents(chunk.getEventsList()));
                logger.info("read {} entries from chunk {} ({} bytes) into {}", chunk.getEventsCount(), entry.getName(), entry.getSize(), namespace);
                eventsRestored += chunk.getEventsCount();
            }
            return eventsRestored;
        } finally {
            logger.info("restoring {} events into namespace '{}' from archive file {} took {}s",
                    eventsRestored, namespace, archiveFile, TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos));
        }
    }

    private List<Events.Event> toEvents(final List<EventsChunk.Event> eventsList) {
        final List<Events.Event> events = new ArrayList<>();
        for (final EventsChunk.Event event : eventsList) {
            events.add(
                new Events.Event(
                    event.getTimestampMillis(),
                    event.toBuilder().putMetadata(FLAG_RESTORED, "true").getMetadataMap(),
                    event.getDimensionsMap(),
                    !ByteString.EMPTY.equals(event.getPayload())
                            ? event.getPayload().toByteArray()
                            : null
                )
            );
        }
        return events;
    }

    // remove all restored events in this chunk to prevent duplicates
    private void cleanRestoredEvents(final Events events,
                                     final String namespace,
                                     final Path archiveFile) throws IOException {
        final String filename = archiveFile.getFileName().toString();
        final Matcher matcher = archiveRegexPattern.matcher(filename);
        if (matcher.matches()) {
            final long start = Long.parseLong(matcher.group("start"));
            final long end = Long.parseLong(matcher.group("end"));
            final HashMap<String, String> dimensionMap = new HashMap<>();
            dimensionMap.put(isRestoredFlag, "1");
            events.delete(namespace, start, end, null, dimensionMap);
        }
    }

    // retrieves all archive files that overlap with the timeframe
    public List<Path> getFileArchiveList(final String namespace,
                                         final long startTimestampMillis,
                                         final long endTimestampMillis) throws IOException {
        final long windowStart = getFloorForChunk(startTimestampMillis);
        final long windowEnd = (endTimestampMillis <= Long.MAX_VALUE - this.chunkMillis)
                ? getCeilingForChunk(endTimestampMillis)
                : endTimestampMillis;
        return Files.list(Paths.get(this.baseDirectory, this.subDirectory))
            .filter(path -> {
                // filter to archive files that overlap with the timeframe
                final String filename = path.getFileName().toString();
                final Matcher matcher = archiveRegexPattern.matcher(filename);
                if (matcher.matches() && matcher.group("namespace").equals(namespace)) {
                    final long fileStart = Long.parseLong(matcher.group("start"));
                    final long fileEnd = Long.parseLong(matcher.group("end"));
                    // -------s-------------e---------  <- start and end parameters
                    // ssssssssssssssssssssss           <- first check
                    //        eeeeeeeeeeeeeeeeeeeeeeee  <- second check
                    // any combination of s and e the file overlaps the timeframe
                    return fileStart <= windowEnd && fileEnd >= windowStart;
                }
                return false;
            }).collect(Collectors.toList());
    }

    // resolve archive filename
    public Path getFileArchive(final String namespace, final long chunkStartMillis) {
        return getFile(archivePathFormat,
                namespace,
                chunkStartMillis,
                chunkStartMillis + chunkMillis - 1);
    }
}
