package com.salesforce.cantor.archive.file;

import com.salesforce.cantor.misc.archivable.Archiver;
import com.salesforce.cantor.misc.archivable.EventsArchiver;
import com.salesforce.cantor.misc.archivable.ObjectsArchiver;
import com.salesforce.cantor.misc.archivable.SetsArchiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

/**
 * An implementation of the archiver which stores data on disk.
 *
 * {@code baseDirectory} and {@code chunkMillis} can specified or will default to
 * {@link FileArchiver#defaultArchivePathBase} and {@link FileArchiver#defaultChunkMillis} respectively
 */
public class FileArchiver implements Archiver {
    private static final Logger logger = LoggerFactory.getLogger(FileArchiver.class);

    protected static final String defaultArchivePathBase = "/tmp/cantor-archive";
    protected static final int defaultChunkCount = 1_000;
    protected static final long defaultChunkMillis = TimeUnit.HOURS.toMillis(1);

    private final FileSetsArchiver setsArchive;
    private final FileObjectsArchiver objectsArchive;
    private final FileEventsArchiver eventsArchive;

    public FileArchiver() {
        this(defaultArchivePathBase);
    }

    public FileArchiver(final String baseDirectory) {
        this(baseDirectory, defaultChunkCount, defaultChunkMillis);
    }

    public FileArchiver(final String baseDirectory, final int archiveChunkCount, final long eventsChunkMillis) {
        checkArgument(baseDirectory != null && !baseDirectory.isEmpty(), "null/empty baseDirectory");
        checkArgument(archiveChunkCount > 0, "archiveChunkCount must be greater than zero");
        checkArgument(eventsChunkMillis > 0, "eventsChunkMillis must be greater than zero");
        logger.info("initializing file archiver with directory '{}' in {}ms chunks", baseDirectory, eventsChunkMillis);

        final File createDirectory = new File(baseDirectory);
        if (!createDirectory.mkdirs() && !createDirectory.exists()) {
            throw new IllegalStateException("Failed to create base directory for file archive: " + baseDirectory);
        }

        this.setsArchive = new FileSetsArchiver(baseDirectory, archiveChunkCount);
        this.objectsArchive = new FileObjectsArchiver(baseDirectory, archiveChunkCount);
        this.eventsArchive = new FileEventsArchiver(baseDirectory, eventsChunkMillis);
    }

    @Override
    public SetsArchiver setsArchiver() {
        return this.setsArchive;
    }

    @Override
    public ObjectsArchiver objectsArchiver() {
        return this.objectsArchive;
    }

    @Override
    public EventsArchiver eventsArchiver() {
        return this.eventsArchive;
    }
}
