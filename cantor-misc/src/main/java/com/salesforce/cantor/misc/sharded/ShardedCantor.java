/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.misc.sharded;

import com.salesforce.cantor.*;

import static com.salesforce.cantor.common.CommonPreconditions.checkArgument;

/**
 * The ShardedCantor implementation provides sharding on top of a number of delegate Cantor instances.
 * Consistent hashing is used to shard calls between different delegates based on the namespace, so all calls
 * to one namespace goes to a single delegate Cantor instance.
 *
 * Use it like this:
 *
 * Cantor[] delegates = ...
 * Cantor cantor = new ShardedCantor(delegates);
 */
public class ShardedCantor implements Cantor {
    private final ShardedObjects objects;
    private final ShardedSets sets;
    private final ShardedEvents events;

    public ShardedCantor(final Cantor... delegates) {
        checkArgument(delegates != null && delegates.length > 0, "null/empty delegates");

        final Objects[] objects = new Objects[delegates.length];
        final Sets[] sets = new Sets[delegates.length];
        final Events[] events = new Events[delegates.length];
        for (int i = 0; i < delegates.length; ++i) {
            objects[i] = delegates[i].objects();
            sets[i] = delegates[i].sets();
            events[i] = delegates[i].events();
        }
        this.objects = new ShardedObjects(objects);
        this.sets = new ShardedSets(sets);
        this.events = new ShardedEvents(events);
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
