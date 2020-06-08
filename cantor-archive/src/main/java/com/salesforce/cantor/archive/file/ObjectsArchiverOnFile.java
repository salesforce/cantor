/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive.file;

import com.google.protobuf.ByteString;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.archive.ObjectsChunk;
import com.salesforce.cantor.misc.archivable.ObjectsArchiver;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

public class ObjectsArchiverOnFile extends AbstractBaseArchiverOnFile implements ObjectsArchiver {
    private static final Logger logger = LoggerFactory.getLogger(ObjectsArchiverOnFile.class);
    protected static final String archivePathFormat = "/archive-objects-%s-%d";

    public static final int MAX_CHUNK_SIZE = 1_000;

    public ObjectsArchiverOnFile(final String baseDirectory, final int archiveChunkCount) {
        super(baseDirectory, archiveChunkCount);
    }

    @Override
    public boolean hasArchives(final String namespace) {
        return false;
    }

    @Override
    public void archive(final Objects objects, final String namespace) throws IOException {
    }

    @Override
    public void restore(final Objects objects, final String namespace) throws IOException {
    }

    public Path getFileArchive(final String namespace) {
        return getFile(archivePathFormat,
                namespace,
                this.chunkCount);
    }
}
