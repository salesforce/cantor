/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc.admin;

import com.salesforce.cantor.*;
import com.salesforce.cantor.grpc.*;
import io.grpc.ManagedChannel;

public class CantorOnGrpcAdmin implements Cantor {
    private final Objects objects;
    private final Sets sets;
    private final Events events;
    private final ManagementOnGrpc manager;

    public CantorOnGrpcAdmin(final ManagedChannel channel) {
        this.objects = new ObjectsOnGrpc(channel);
        this.sets = new SetsOnGrpc(channel);
        this.events = new EventsOnGrpc(channel);
        this.manager = new ManagementOnGrpc(channel);
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

    public ManagementOnGrpc manager() {
        return this.manager;
    }
}
