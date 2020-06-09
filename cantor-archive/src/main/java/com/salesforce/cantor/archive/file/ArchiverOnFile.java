package com.salesforce.cantor.archive.file;

import com.salesforce.cantor.misc.archivable.CantorArchiver;
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
 * {@link ArchiverOnFile#defaultArchivePathBase} and {@link ArchiverOnFile#defaultChunkMillis} respectively
 */
public class ArchiverOnFile implements CantorArchiver {
    private static final Logger logger = LoggerFactory.getLogger(ArchiverOnFile.class);

    protected static final String defaultArchivePathBase = "/tmp/cantor-archive";
    protected static final long defaultChunkMillis = TimeUnit.HOURS.toMillis(1);

    private final SetsArchiverOnFile setsArchive;
    private final ObjectsArchiverOnFile objectsArchive;
    private final EventsArchiverOnFile eventsArchive;

    public ArchiverOnFile() {
        this(defaultArchivePathBase);
    }

    public ArchiverOnFile(final String baseDirectory) {
        this(baseDirectory, defaultChunkMillis);
    }

    public ArchiverOnFile(final String baseDirectory, final long eventsChunkMillis) {
        checkArgument(baseDirectory != null && !baseDirectory.isEmpty(), "null/empty baseDirectory");
        checkArgument(eventsChunkMillis > 0, "eventsChunkMillis must be greater than zero");
        logger.info("initializing file archiver with directory '{}' in {}ms chunks", baseDirectory, eventsChunkMillis);

        final File createDirectory = new File(baseDirectory);
        if (!createDirectory.mkdirs() && !createDirectory.exists()) {
            throw new IllegalStateException("Failed to create base directory for file archive: " + baseDirectory);
        }

        this.setsArchive = new SetsArchiverOnFile(baseDirectory);
        this.objectsArchive = new ObjectsArchiverOnFile(baseDirectory);
        this.eventsArchive = new EventsArchiverOnFile(baseDirectory, eventsChunkMillis);
    }

    @Override
    public SetsArchiver sets() {
        return this.setsArchive;
    }

    @Override
    public ObjectsArchiver objects() {
        return this.objectsArchive;
    }

    @Override
    public EventsArchiver events() {
        return this.eventsArchive;
    }
}
