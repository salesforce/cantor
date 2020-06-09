/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive.file;

import com.salesforce.cantor.Sets;
import com.salesforce.cantor.misc.archivable.SetsArchiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

public class SetsArchiverOnFile extends AbstractBaseArchiverOnFile implements SetsArchiver {
    private static final Logger logger = LoggerFactory.getLogger(SetsArchiverOnFile.class);
    private static final String archivePathFormat = "/archive-sets-%s";

    public static final int MAX_CHUNK_SIZE = 1_000;

    public SetsArchiverOnFile(final String baseDirectory) {
        super(baseDirectory);
    }

    @Override
    public boolean hasArchives(final String namespace, final String set) {
        return false;
    }

    @Override
    public void archive(final Sets sets, final String namespace) throws IOException {
    }

    @Override
    public void restore(final Sets sets, final String namespace) throws IOException {
    }

    public Path getFileArchive(final String namespace) {
        return getFile(archivePathFormat, namespace);
    }
}
