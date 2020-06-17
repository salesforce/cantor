/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.archive.file;

import com.salesforce.cantor.Objects;
import com.salesforce.cantor.misc.archivable.ObjectsArchiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

public class ObjectsArchiverOnFile extends AbstractBaseArchiverOnFile implements ObjectsArchiver {
    private static final Logger logger = LoggerFactory.getLogger(ObjectsArchiverOnFile.class);
    protected static final String archivePathFormat = "/archive-objects-%s";

    public static final int maxChunkSize = 1_000;

    public ObjectsArchiverOnFile(final String baseDirectory) {
        super(baseDirectory);
    }

    @Override
    public void archive(final Objects objects, final String namespace) throws IOException {
    }

    @Override
    public void restore(final Objects objects, final String namespace) throws IOException {
    }

    public Path getFileArchive(final String namespace) {
        return getFile(archivePathFormat, namespace);
    }
}
