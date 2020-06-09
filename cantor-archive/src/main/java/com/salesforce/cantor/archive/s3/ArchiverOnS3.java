package com.salesforce.cantor.archive.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.salesforce.cantor.archive.file.ArchiverOnFile;
import com.salesforce.cantor.misc.archivable.CantorArchiver;
import com.salesforce.cantor.misc.archivable.EventsArchiver;
import com.salesforce.cantor.misc.archivable.ObjectsArchiver;
import com.salesforce.cantor.misc.archivable.SetsArchiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.common.CommonPreconditions.checkString;

/**
 * An implementation of the archiver which stores data on an S3 instance.
 */
public class ArchiverOnS3 implements CantorArchiver {
    private static final Logger logger = LoggerFactory.getLogger(ArchiverOnS3.class);

    protected static final String defaultBucketName = "cantor-archive";
    protected static final long defaultChunkMillis = TimeUnit.HOURS.toMillis(1);

    private final SetsArchiverOnS3 setsArchive;
    private final ObjectsArchiverOnS3 objectsArchive;
    private final EventsArchiverOnS3 eventsArchive;

    public ArchiverOnS3(final AmazonS3 s3Client) {
        this(s3Client, defaultBucketName, defaultChunkMillis);
    }

    public ArchiverOnS3(final AmazonS3 s3Client, final String bucketName) {
        this(s3Client, bucketName, defaultChunkMillis);
    }

    public ArchiverOnS3(final AmazonS3 s3Client, final String bucketName, final long chunkMillis) {
        checkArgument(s3Client != null, "null/empty s3Client");
        checkString(bucketName, "bucketName is null or empty");
        logger.info("initializing s3 archiver in bucket '{}' with client '{}'", s3Client, bucketName);

        final CantorArchiver fileArchiver = new ArchiverOnFile();
        this.setsArchive = new SetsArchiverOnS3(fileArchiver);
        this.objectsArchive = new ObjectsArchiverOnS3(fileArchiver);
        this.eventsArchive = new EventsArchiverOnS3(fileArchiver, chunkMillis);
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
