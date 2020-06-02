package com.salesforce.cantor.misc.archivable;

import com.salesforce.cantor.Events;

import java.io.IOException;
import java.util.Map;

/**
 * EventsArchiver is the contract used by {@link ArchivableEvents} when handling an archive
 * @param <T>
 *     The store media that will be used to archive
 */
public interface EventsArchiver<T> {

    /**
     * Will retrieve and archive all events using these given parameters and load this into the destination location in
     * buckets specified by the chuck interval.
     */
    void archive(final Events events,
                 final String namespace,
                 final long startTimestampMillis,
                 final long endTimestampMillis,
                 final Map<String, String> metadataQuery,
                 final Map<String, String> dimensionsQuery,
                 final long chunkMillis) throws IOException;
}
