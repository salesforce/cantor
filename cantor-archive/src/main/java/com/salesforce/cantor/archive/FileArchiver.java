package com.salesforce.cantor.archive;

import com.salesforce.cantor.misc.archivable.Archiver;
import com.salesforce.cantor.misc.archivable.EventsArchiver;

import java.nio.file.Path;

public class FileArchiver implements Archiver<Path> {
    @Override
    public EventsArchiver<Path> eventsArchiver() {
        return new FileEventsArchiver();
    }
}
