/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive.s3;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Sets;
import com.salesforce.cantor.archive.file.AbstractBaseArchiverOnFile;
import com.salesforce.cantor.misc.archivable.SetsArchiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

public class SetsArchiverOnS3 extends AbstractBaseArchiverOnFile implements SetsArchiver {
    private static final Logger logger = LoggerFactory.getLogger(SetsArchiverOnS3.class);
    private static final String archiveFilename = "archive-sets-%s-%d";

    public static final int MAX_CHUNK_SIZE = 1_000;

    public SetsArchiverOnS3(final Cantor s3Cantor, final String baseDirectory) {
        super(baseDirectory);
    }

    @Override
    public void archive(final Sets sets, final String namespace) throws IOException {
        // not implemented yet
    }

    @Override
    public void restore(final Sets sets, final String namespace) throws IOException {
        // not implemented yet
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        // not implemented yet
        return null;
    }

    @Override
    public void create(final String namespace) throws IOException {
        // not implemented yet
    }

    @Override
    public void drop(final String namespace) throws IOException {
        // not implemented yet
    }
}
