/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive.s3;

import com.salesforce.cantor.Sets;
import com.salesforce.cantor.misc.archivable.CantorArchiver;
import com.salesforce.cantor.misc.archivable.SetsArchiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SetsArchiverOnS3 extends AbstractBaseArchiverOnS3 implements SetsArchiver {
    private static final Logger logger = LoggerFactory.getLogger(SetsArchiverOnS3.class);
    private static final String archiveFilename = "archive-sets-%s-%d";

    public static final int MAX_CHUNK_SIZE = 1_000;

    public SetsArchiverOnS3(final CantorArchiver fileArchiver) {
        super(fileArchiver);
    }

    @Override
    public void archive(final Sets sets, final String namespace) throws IOException {
    }

    @Override
    public void restore(final Sets sets, final String namespace) throws IOException {
    }
}
