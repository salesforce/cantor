package com.salesforce.cantor.archive.s3;

import com.salesforce.cantor.archive.file.ArchiverOnFile;
import com.salesforce.cantor.misc.archivable.EventsArchiver;
import com.salesforce.cantor.misc.archivable.ObjectsArchiver;
import com.salesforce.cantor.misc.archivable.SetsArchiver;
import com.salesforce.cantor.s3.CantorOnS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

/**
 * An implementation of the archiver which stores data on an S3 instance.
 */
public class ArchiverOnS3 extends ArchiverOnFile {
    private static final Logger logger = LoggerFactory.getLogger(ArchiverOnS3.class);
    private static final String defaultArchivePathBase = "cantor-s3-archive-data";
    private static final long defaultChunkMillis = TimeUnit.HOURS.toMillis(1);

    private final SetsArchiverOnS3 setsArchive;
    private final ObjectsArchiverOnS3 objectsArchive;
    private final EventsArchiverOnS3 eventsArchive;

    public ArchiverOnS3(final CantorOnS3 cantor) throws IOException {
        this(cantor, defaultChunkMillis);
    }

    public ArchiverOnS3(final CantorOnS3 cantor, final long eventsChunkMillis) throws IOException {
        super(defaultArchivePathBase, eventsChunkMillis);
        checkArgument(cantor != null, "null/empty cantor");
        checkArgument(eventsChunkMillis > 0, "eventsChunkMillis must be greater than zero");
        logger.info("initializing s3 archiver with {}ms chunks", eventsChunkMillis);

        this.setsArchive = new SetsArchiverOnS3(cantor, defaultArchivePathBase);
        this.objectsArchive = new ObjectsArchiverOnS3(cantor, defaultArchivePathBase);
        this.eventsArchive = new EventsArchiverOnS3(cantor, defaultArchivePathBase, eventsChunkMillis);
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
