package com.salesforce.cantor.archive.s3;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.misc.archivable.CantorArchiver;

public abstract class AbstractBaseArchiverOnS3 {
    protected final Cantor cantorOnS3;
    protected final CantorArchiver fileArchiver;
    protected final long chunkMillis;

    public AbstractBaseArchiverOnS3(final Cantor cantorOnS3, final CantorArchiver fileArchiver) {
        this(cantorOnS3, fileArchiver, 0);
    }

    public AbstractBaseArchiverOnS3(final Cantor cantorOnS3, final CantorArchiver fileArchiver, final long chunkMillis) {
        this.cantorOnS3 = cantorOnS3;
        this.fileArchiver = fileArchiver;
        this.chunkMillis = chunkMillis;
    }

    protected long getFloorForChunk(final long timestampMillis) {
        return (timestampMillis / this.chunkMillis) * this.chunkMillis;
    }

    protected long getCeilingForChunk(final long timestampMillis) {
        if (timestampMillis >= Long.MAX_VALUE - this.chunkMillis) {
            return Long.MAX_VALUE;
        }
        return getFloorForChunk(timestampMillis) + this.chunkMillis - 1;
    }
}
