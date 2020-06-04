package com.salesforce.cantor.misc.archivable;

import com.salesforce.cantor.Events;

import java.io.IOException;
import java.util.Map;

/**
 * EventsArchiver is the contract used by {@link ArchivableEvents} when handling an archive
 */
public interface EventsArchiver {

    /**
     *  Checks for the existence of any archives that hold events in the provided timeframe.
     *  <br><br>
     *  {@code hasArchives()} is expected return {@literal true} if even one archived chunk overlaps with the timeframe,
     *  and {@literal false} otherwise.
     *  <br><br>
     *  Depending on the implementation {@code restore()} could be called even if the exact query doesn't match any
     *  events in the archive.
     */
    boolean hasArchives(final Events delegate,
                        final String namespace,
                        final long startTimestampMillis,
                        final long endTimestampMillis);

    /**
     * Will retrieve and archive all events using these given parameters and load this into the destination.
     */
    void archive(final Events events,
                 final String namespace,
                 final long startTimestampMillis,
                 final long endTimestampMillis,
                 final Map<String, String> metadataQuery,
                 final Map<String, String> dimensionsQuery) throws IOException;

    /**
     * Will retrieve and archive all events before the provided timestamp.
     * <br><br>
     * {@code archive()} may not necessarily store every event up to the {@code endTimestampMillis} as chunking of the
     * data into buckets may be used.
     * <br><br>
     * It will depend on the implementation.
     */
    void archive(final Events events,
                 final String namespace,
                 final long endTimestampMillis) throws IOException;

    /**
     * Will retrieve all archived chunks between the provided timestamps and load them back into Cantor.
     * <br><br>
     * {@code restore()} may pull more events down than what are between the {@code startTimestampMillis} and
     * {@code endTimestampMillis} as it may restore surrounding data if the implementation uses chunking.
     * <br><br>
     * It will depend on the implementation
     */
    void restore(final Events events,
                 final String namespace,
                 final long startTimestampMillis,
                 final long endTimestampMillis) throws IOException;
}
