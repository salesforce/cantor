/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.loggable;

import com.salesforce.cantor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

/**
 * The LoggableCantor implementation is a wrapper around a delegate Cantor instance, adding
 * logging to each method call. The log line contains the method called along with parameters and
 * time spent on the call.
 *
 * Use it like this:
 *
 * Cantor delegate = ...
 * Cantor cantor = new LoggableCantor(delegate);
 */
public class LoggableCantor implements Cantor {
    private static final Logger logger = LoggerFactory.getLogger(LoggableCantor.class);

    private final Objects objects;
    private final Sets sets;
    private final Maps maps;
    private final Events events;

    public LoggableCantor(final Cantor delegate) {
        checkArgument(delegate != null, "null delegate");

        logger.info("new instance of loggable cantor created");

        this.objects = new LoggableObjects(delegate.objects());
        this.sets = new LoggableSets(delegate.sets());
        this.maps = new LoggableMaps(delegate.maps());
        this.events = new LoggableEvents(delegate.events());
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
    public Maps maps() {
        return this.maps;
    }

    @Override
    public Events events() {
        return this.events;
    }
}
