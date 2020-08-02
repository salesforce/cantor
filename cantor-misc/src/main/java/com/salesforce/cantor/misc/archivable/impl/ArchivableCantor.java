/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.archivable.impl;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.Sets;
import com.salesforce.cantor.misc.archivable.CantorArchiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

/**
 * The ArchivableCantor implementation is a wrapper around a delegate Cantor instance and a {@link CantorArchiver} reference.
 * The {@code delegate} is the main data-store which is used directly for {@link Sets} and {@link Objects}.
 * The {@code archiveDelegate} is used to archive any events removed when calling {@link Events#expire} for
 * longer term storage.
 * <br/>
 * Use it like this:
 * <pre>
 * Cantor delegate = ...
 * Archiver archiver = ...
 * Cantor cantor = new ArchivableCantor(delegate, archiver);
 * </pre>
 */
public class ArchivableCantor implements Cantor {
    private static final Logger logger = LoggerFactory.getLogger(ArchivableCantor.class);

    private final Objects objects;
    private final Sets sets;
    private final Events events;

    public ArchivableCantor(final Cantor delegate, final CantorArchiver archiver) {
        checkArgument(delegate != null, "null delegate");
        checkArgument(archiver != null, "null archiver");

        logger.info("new instance of archivable cantor created");

        //TODO: add support for ArchivableObjects and ArchivableSets
        this.objects = delegate.objects();
        this.sets = delegate.sets();
        this.events = new ArchivableEvents(delegate.events(), archiver.events());
    }

    @Override
    public Objects objects() {
        return this.objects;
    }

    @Override
    public Sets sets() {
        return this.sets;
    }

    @Override
    public Events events() {
        return this.events;
    }
}
