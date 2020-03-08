/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.async;

import com.salesforce.cantor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

public class AsyncCantor implements Cantor {
    private static final Logger logger = LoggerFactory.getLogger(AsyncCantor.class);

    private final AsyncObjects objects;
    private final AsyncSets sets;
    private final AsyncMaps maps;
    private final AsyncEvents events;

    public AsyncCantor(final Cantor delegate, final ExecutorService executorService) {
        checkArgument(delegate != null, "null delegate");
        checkArgument(executorService != null, "null executor service");

        logger.info("new instance of async cantor created");

        this.objects = new AsyncObjects(delegate.objects(), executorService);
        this.sets = new AsyncSets(delegate.sets(), executorService);
        this.maps = new AsyncMaps(delegate.maps(), executorService);
        this.events = new AsyncEvents(delegate.events(), executorService);
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
