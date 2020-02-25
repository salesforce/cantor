/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.salesforce.cantor.*;

public class CantorOnGrpc implements Cantor {
    private final Objects objects;
    private final Sets sets;
    private final Maps maps;
    private final Events events;

    public CantorOnGrpc(final String target) {
        this.objects = new ObjectsOnGrpc(target);
        this.sets = new SetsOnGrpc(target);
        this.maps = new MapsOnGrpc(target);
        this.events = new EventsOnGrpc(target);
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
