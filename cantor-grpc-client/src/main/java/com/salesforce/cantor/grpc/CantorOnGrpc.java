/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.grpc;

import com.salesforce.cantor.*;
import com.salesforce.cantor.grpc.auth.utils.Credentials;
import io.grpc.ManagedChannel;

public class CantorOnGrpc implements Cantor {
    private final Objects objects;
    private final Sets sets;
    private final Events events;

    public CantorOnGrpc(final String target) {
        this.objects = new ObjectsOnGrpc(target);
        this.sets = new SetsOnGrpc(target);
        this.events = new EventsOnGrpc(target);
    }

    public CantorOnGrpc(final String target, final Credentials credentials) {
        this.objects = new ObjectsOnGrpc(target, credentials);
        this.sets = new SetsOnGrpc(target, credentials);
        this.events = new EventsOnGrpc(target, credentials);
    }

    public CantorOnGrpc(final ManagedChannel channel) {
        this.objects = new ObjectsOnGrpc(channel);
        this.sets = new SetsOnGrpc(channel);
        this.events = new EventsOnGrpc(channel);
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
