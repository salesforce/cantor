/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.archivable;

import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

/**
 * The ArchivableCantor implementation is a wrapper around a delegate Cantor instance and an {@link Archiver} reference.
 * The {@code delegate} is the main data-store with direct implementations of {@link Sets} and {@link Objects}.
 * The {@code archiveDelegate} is used to archive any events removed when calling {@link Events#expire} or
 * {@link Events#delete} for longer term storage.
 * <br/>
 * Future Work: Calls to retrieve data that has been archived will be loaded back into the {@code delegate} Cantor.
 * <br/>
 * Use it like this:
 * <pre>
 * Cantor delegate = ...
 * Archiver<?> archiveDelegate = ...
 * Cantor cantor = new ArchivableCantor(delegate, archiveDelegate);
 * </pre>
 */
public class ArchivableCantor implements Cantor {
    private static final Logger logger = LoggerFactory.getLogger(ArchivableCantor.class);

    private final Objects objects;
    private final Sets sets;
    private final Events events;

    public ArchivableCantor(final Cantor delegate, final Archiver<?> archiveDelegate) {
        checkArgument(delegate != null, "null delegate");
        checkArgument(archiveDelegate != null, "null archiveDelegate");

        logger.info("new instance of loggable cantor created");

        //TODO: add support for ArchivableObjects and ArchivableSets
        this.objects = delegate.objects();
        this.sets = delegate.sets();
        this.events = new ArchivableEvents(delegate.events(), archiveDelegate.eventsArchiver());
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
