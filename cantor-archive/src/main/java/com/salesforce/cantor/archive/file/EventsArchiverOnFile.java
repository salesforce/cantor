/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive.file;

import com.google.protobuf.ByteString;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.archive.EventsChunk;
import com.salesforce.cantor.common.EventsPreconditions;
import com.salesforce.cantor.misc.archivable.EventsArchiver;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;
import static com.salesforce.cantor.common.CommonPreconditions.checkNamespace;

public class EventsArchiverOnFile extends AbstractBaseArchiverOnFile implements EventsArchiver {
    private static final Logger logger = LoggerFactory.getLogger(EventsArchiverOnFile.class);
    private static final String archivePathFormat = "/archive-events-%s-%d-%d";

    public EventsArchiverOnFile(final String baseDirectory, final long chunkMillis) {
        super(baseDirectory, chunkMillis);
    }

    @Override
    public boolean hasArchives(final String namespace,
                               final long startTimestampMillis,
                               final long endTimestampMillis) throws IOException {
        return false;
    }

    @Override
    public void archive(final Events events,
                        final String namespace,
                        final long startTimestampMillis,
                        final long endTimestampMillis,
                        final Map<String, String> metadataQuery,
                        final Map<String, String> dimensionsQuery) throws IOException {
    }

    @Override
    public void restore(final Events events,
                        final String namespace,
                        final long startTimestampMillis,
                        final long endTimestampMillis) throws IOException {
    }

    public Path getFileArchive(final String namespace, final long startTimestampMillis) {
        return getFile(archivePathFormat,
                namespace,
                startTimestampMillis,
                startTimestampMillis + chunkMillis - 1);
    }
}
