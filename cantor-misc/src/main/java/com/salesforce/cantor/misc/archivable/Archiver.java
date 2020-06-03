package com.salesforce.cantor.misc.archivable;

/**
 * Abstraction for accessing all underlying archives
 */
public interface Archiver {

    /**
     * Returns an instance of SetsArchiver.
     *
     * @return instance of sets archiver
     */
    SetsArchiver setsArchiver();

    /**
     * Returns an instance of objectsArchiver.
     *
     * @return instance of objects archiver
     */
    ObjectsArchiver objectsArchiver();

    /**
     * Returns an instance of EventsArchiver.
     *
     * @return instance of events archiver
     */
    EventsArchiver eventsArchiver();
}
