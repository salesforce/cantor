/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.metrics;

import com.codahale.metrics.MetricRegistry;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.Maps;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.Sets;

public class MetricCollectingCantor implements Cantor {
    private final Objects objects;
    private final Sets sets;
    private final Maps maps;
    private final Events events;

    public MetricCollectingCantor(final MetricRegistry metrics, final Cantor delegate) {
        this.objects = new MetricCollectingObjects(metrics, delegate.objects());
        this.sets = new MetricCollectingSets(metrics, delegate.sets());
        this.maps = new MetricCollectingMaps(metrics, delegate.maps());
        this.events = new MetricCollectingEvents(metrics, delegate.events());
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
