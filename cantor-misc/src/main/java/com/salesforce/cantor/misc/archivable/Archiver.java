package com.salesforce.cantor.misc.archivable;

/**
 * Abstraction for accessing all underlying archives
 */
public interface Archiver<T> {

    // TODO: add support for Objects and Sets; should we all archivers of different types in the same wrapper?

    /**
     * Returns an instance of EventsArchiver.
     *
     * @return instance of events archiver
     */
    EventsArchiver<T> eventsArchiver();
}
