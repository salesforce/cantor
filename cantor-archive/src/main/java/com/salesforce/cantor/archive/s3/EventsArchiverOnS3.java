/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive.s3;

import com.salesforce.cantor.Events;
import com.salesforce.cantor.misc.archivable.CantorArchiver;
import com.salesforce.cantor.misc.archivable.EventsArchiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class EventsArchiverOnS3 extends AbstractBaseArchiverOnS3 implements EventsArchiver {
    private static final Logger logger = LoggerFactory.getLogger(EventsArchiverOnS3.class);
    private static final String archiveFilename = "archive-events-%s-%d-%d";

    public EventsArchiverOnS3(final CantorArchiver fileArchiver, final long chunkMillis) {
        super(fileArchiver, chunkMillis);
        logger.info("events archiver chucking in {}ms chunks", chunkMillis);
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
}
